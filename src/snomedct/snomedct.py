"""Snomed transformation functionality.

Can be called as a script using the main function.

"""

import logging
from pathlib import Path

import polars as pl
from attrs import fields

from snomedct.datamodel import (
    Config,
    Dataset,
    Metadata,
)
from snomedct.logging_config import get_logger

# Used in dynamic lookup
from snomedct.snomedct_datamodel import SnomedTableConcept  # noqa: F401
from snomedct.snomedct_datamodel import SnomedTableDefinition  # noqa: F401
from snomedct.snomedct_datamodel import SnomedTableDescription  # noqa: F401
from snomedct.snomedct_datamodel import SnomedTableLanguage  # noqa: F401
from snomedct.snomedct_datamodel import SnomedTableRelationship  # noqa: F401
from snomedct.snomedct_datamodel import (
    SnomedData,
    SnomedDataRaw,
)
from snomedct.utils import attrs_to_polars_schema, get_current_function_name

logging.getLogger().setLevel("DEBUG")
logger = get_logger(__name__)


def read_file_to_polars(filename: str, table_schema: type) -> pl.DataFrame:
    """Read snomed tsv file into polars dataframe.

    Uses schema derived from attrs class definition in `snomedct_datamodel`.
    """
    return pl.read_csv(
        filename,
        separator="\t",
        quote_char=None,
        columns=[f.name for f in fields(table_schema)],
        schema_overrides=attrs_to_polars_schema(table_schema),
        null_values=[],
    )


def validate_table(config: Config, table_type: type, table: pl.DataFrame) -> None:
    """Validate dataframe for table by transforming each row to attrs class instance."""
    for row in table.rows(named=True):
        try:
            table_type(**row)
        except (TypeError, ValueError) as e:
            logger.warning("Error in validation of %r for row:", table_type)
            logger.warning(row)
            if not config.safe_validation:
                raise ValueError from e


def generate_dataset_field(config: Config, table_basename: str, filenames: list[str]) -> tuple[str, pl.DataFrame]:
    """Generate dataframe for one of the snomed tables defined in the SnomedDatasetRaw class."""
    logger.debug("Load table %r to polars", table_basename)
    key = table_basename.replace("Snapshot", "").lstrip("_").replace("-", "_").replace("__", "_").lower()
    schema: type = globals()[f"SnomedTable{key.split('_')[0].capitalize()}"]

    file_matches = [f for f in filenames if table_basename in f]

    if not file_matches:
        logger.error("No match for table basename %r", table_basename)

    if len(file_matches) > 1:
        logger.error("multiple matches for pattern %s", table_basename)
        logger.error(file_matches)
        raise ValueError

    table_df = read_file_to_polars(file_matches[0], schema)

    replace_values = config.extras.table_replace_values.get(schema.__name__)
    if replace_values:
        logger.debug("Replacing values in table %r", table_basename)
        table_df = table_df.with_columns(replace_values)

    if config.validate:
        logger.info("Validating table %r", table_basename)
        validate_table(config, schema, table_df)

    return key, table_df


def snomed_load_dataset_from_files(config: Config) -> Dataset:
    """Load tsv files from snomed release packages into dataframes and produce dataset."""
    logger.info("Load raw dataset from files")

    if not Path("input").exists():
        logger.error("Input directory not found.")
        raise FileNotFoundError

    filenames = [str(path) for path in Path().glob(config.extras.glob_pattern)]
    if not filenames:
        logger.error("No release packages found in input dir.")
        raise FileNotFoundError

    data = SnomedDataRaw(
        config=config,
        **dict(generate_dataset_field(config, table_basename, filenames) for table_basename in config.extras.tables),
    )

    return Dataset(
        metadata=Metadata(
            title="Snomed raw dataset",
            description="Dataset loaded from relevant snapshot tsv files in snomed release packages",
            type_=type(data),
            source=get_current_function_name(),
        ),
        config=config,
        data=data,
    )


def denormalize_term_tables(datasetraw: SnomedDataRaw) -> pl.DataFrame:
    """Transform raw snomed term tables into easier to process table format."""
    # Join language refset for bokmål and bokmål general practicioner
    gp_annotated_nb = (
        datasetraw.language_nb_no.join(datasetraw.language_nb_gp_no, on="referencedComponentId", how="left")
        .drop("active_right")
        .rename({"acceptabilityId_right": "acceptabilityId_gp"})
        .with_columns(pl.lit("nb").alias("lc"))
    )

    # Join language refset for nynorsk and nynorsk general practicioner
    gp_annotated_nn = (
        datasetraw.language_nn_no.join(datasetraw.language_nn_gp_no, on="referencedComponentId", how="left")
        .drop("active_right")
        .rename({"acceptabilityId_right": "acceptabilityId_gp"})
        .with_columns(pl.lit("nn").alias("lc"))
    )

    # Languge refet for norwegian
    joined_no_language = pl.concat([gp_annotated_nb, gp_annotated_nn])

    # Merge descripions with language refset data to denormalize terms
    terms_no = (
        datasetraw.description_no_no.join(
            joined_no_language,
            left_on="id",
            right_on="referencedComponentId",
            how="right",
        )
        .filter(pl.col("typeId").is_in(["fsn", "synonym"]))
        .rename({"referencedComponentId": "termId"})
        .select(
            "conceptId",
            "termId",
            "term",
            "lc",
            "typeId",
            "acceptabilityId",
            "acceptabilityId_gp",
        )
    )

    # Join english descriptions from the Norwegian release package and the international release package
    joined_en_descriptions = pl.concat([datasetraw.description_en_no, datasetraw.description_en_int], how="vertical")
    joined_en_language = pl.concat([datasetraw.language_en_no, datasetraw.language_en_int], how="vertical")

    # Merge descripions with language refset data to denormalize terms
    terms_en = (
        joined_en_descriptions.join(
            joined_en_language,
            left_on="id",
            right_on="referencedComponentId",
            how="right",
        )
        .filter(pl.col("typeId").is_in(["fsn", "synonym"]))
        .rename({"refsetId": "lc", "referencedComponentId": "termId"})
        .with_columns(pl.lit(None).alias("acceptabilityId_gp"))
        .select(
            "conceptId",
            "termId",
            "term",
            "lc",
            "typeId",
            "acceptabilityId",
            "acceptabilityId_gp",
        )
    )

    return pl.concat([terms_no, terms_en], how="vertical")


def denormalize_definition_tables(datasetraw: SnomedDataRaw) -> pl.DataFrame:
    """Transform raw snomed definition tables into easier to process table format."""
    joined_no_language = pl.concat(
        [
            datasetraw.language_nb_no.with_columns(pl.lit("nb").alias("lc")),
            datasetraw.language_nb_gp_no.with_columns(pl.lit("nb").alias("lc")),
            datasetraw.language_nn_no.with_columns(pl.lit("nn").alias("lc")),
            datasetraw.language_nn_gp_no.with_columns(pl.lit("nn").alias("lc")),
        ],
    )

    definitions_no = (
        datasetraw.definition_no_no.join(
            joined_no_language,
            left_on="id",
            right_on="referencedComponentId",
            how="right",
        )
        .filter(pl.col("typeId") == "definition")
        .select("conceptId", "referencedComponentId", "term", "lc")
        .rename({"referencedComponentId": "definitionId"})
    )

    joined_en_definitions = pl.concat([datasetraw.definition_en_no, datasetraw.definition_en_int], how="vertical")
    joined_en_language = pl.concat([datasetraw.language_en_no, datasetraw.language_en_int], how="vertical")

    definitions_en = (
        joined_en_definitions.join(
            joined_en_language,
            left_on="id",
            right_on="referencedComponentId",
            how="right",
        )
        .filter(pl.col("typeId") == "definition")
        .select("conceptId", "referencedComponentId", "term", "refsetId")
        .rename({"refsetId": "lc", "referencedComponentId": "definitionId"})
    )

    return pl.concat([definitions_no, definitions_en])


def snomed_denormalize_dataset(datasetraw: Dataset) -> Dataset:
    """Denormalize snomed dataset for easier processing."""
    logger.info("Denormalize dataset")

    data = SnomedData(
        ids=datasetraw.data.ids,
        terms=denormalize_term_tables(datasetraw.data),
        definitions=denormalize_definition_tables(datasetraw.data),
        relations=pl.concat([datasetraw.data.relationship_no, datasetraw.data.relationship_int], how="vertical"),
    )

    return Dataset(
        metadata=Metadata(
            title="Snomed dataset denormalized",
            description="Denormalized snomed dataset with one table for terms, definitions etc.",
            type_=type(data),
            source=get_current_function_name(),
            provenance=datasetraw.metadata,
        ),
        config=datasetraw.config,
        data=data,
    )
