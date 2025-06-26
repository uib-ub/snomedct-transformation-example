# ruff: noqa: ERA001, N815

"""Datamodel for snomed processing."""

import polars as pl
from attrs import define, field, fields, validators

from snomedct.datamodel import Config
from snomedct.logging_config import get_logger

logger = get_logger(__name__)


@define(frozen=True)
class SnomedExtras:
    """Config extras for snomed processing."""

    id_filter: tuple[str, ...] | None
    tables: tuple
    glob_pattern: str
    table_replace_values: dict[str, list]


@define
class SnomedTableDescription:
    """Description table in snomed release package.

    Docs: https://confluence.ihtsdotools.org/display/DOCRELFMT/4.2.2.+Description+File+Specification
    """

    id: str
    # effectiveTime: int
    active: int = field(validator=validators.in_([1, 0]))
    # moduleId: str
    conceptId: str
    languageCode: str = field(validator=validators.in_(["no", "en"]))
    typeId: str = field(validator=validators.in_(["synonym", "fsn", "definition"]))
    term: str
    # caseSignificanceId: str


SnomedTableDefinition = SnomedTableDescription


@define
class SnomedTableRelationship:
    """Relationship table in snomed release package.

    Docs: https://confluence.ihtsdotools.org/display/DOCRELFMT/4.2.3+Relationship+File+Specification

    Implementation of releations processing and mapping not complete.
    """

    id: str
    # effectiveTime: int
    active: int
    # moduleId: str
    sourceId: str
    destinationId: str
    relationshipGroup: str
    typeId: str
    characteristicTypeId: str
    # modifierId: int


@define
class SnomedTableLanguage:
    """Language table in snomed release package.

    Docs: https://confluence.ihtsdotools.org/display/DOCRELFMT/5.2.2.1+Language+Reference+Set
    """

    id: str
    # effectiveTime: int
    active: int = field(validator=validators.in_([1, 0]))
    # moduleId: str
    refsetId: str  # Used to look up en-us/en-gb in INT refset
    referencedComponentId: str
    acceptabilityId: str = field(validator=validators.in_(["tilrådd", "akseptabel"]))


@define
class SnomedTableConcept:
    """Class for concept table in snomed release package.

    Docs: https://confluence.ihtsdotools.org/display/DOCRELFMT/4.2.1+Concept+File+Specification
    """

    id: str
    # effectiveTime: int
    active: int = field(validator=validators.in_([1, 0]))
    # moduleId: str
    # definitionStatusId


@define(kw_only=True)
class SnomedData:
    """Denormalized snomed data."""

    ids: pl.Series = field()
    terms: pl.DataFrame = field()
    definitions: pl.DataFrame = field()
    relations: pl.DataFrame = field()


@define(kw_only=True)
class SnomedDataRaw:
    """Tables loaded from snomed release packages."""

    config: Config = field()
    ids: pl.Series = field(init=False)
    concept_no: pl.DataFrame = field()
    concept_int: pl.DataFrame = field()
    description_no_no: pl.DataFrame = field()
    description_en_no: pl.DataFrame = field()
    description_en_int: pl.DataFrame = field()
    definition_no_no: pl.DataFrame = field()
    definition_en_no: pl.DataFrame = field()
    definition_en_int: pl.DataFrame = field()
    language_nb_no: pl.DataFrame = field()
    language_nb_gp_no: pl.DataFrame = field()
    language_nn_no: pl.DataFrame = field()
    language_nn_gp_no: pl.DataFrame = field()
    language_en_no: pl.DataFrame = field()
    language_en_int: pl.DataFrame = field()
    relationship_no: pl.DataFrame = field()
    relationship_int: pl.DataFrame = field()

    def __attrs_post_init__(self) -> None:
        """Post-process parsed tables."""
        logger.info("Apply dataset post init processing")

        def filter_table(table: pl.DataFrame, column: str, is_in: pl.Series, *, exclude: bool = False) -> pl.DataFrame:
            if not exclude:
                return table.filter(table[column].is_in(is_in))
            return table.filter(~table[column].is_in(is_in))

        def reduce_table(table: pl.DataFrame, column: str, is_in: pl.Series) -> pl.DataFrame:
            return table.filter(table[column].is_in(is_in))

        # Remove rows from INT table if a row with the same ID is in the corresponding NO table
        self.concept_int = filter_table(self.concept_int, "id", self.concept_no["id"], exclude=True)
        self.description_en_int = filter_table(
            self.description_en_int,
            "id",
            self.description_en_no["id"],
            exclude=True,
        )
        self.definition_en_int = filter_table(self.definition_en_int, "id", self.definition_en_no["id"], exclude=True)
        self.language_en_int = filter_table(self.language_en_int, "id", self.language_en_no["id"], exclude=True)
        self.relationship_int = filter_table(self.relationship_int, "id", self.relationship_no["id"], exclude=True)

        # Filter for active rows
        for attribute in [f for f in fields(self.__class__) if f.name not in ["config", "ids"]]:
            field_value = getattr(self, attribute.name)
            setattr(self, attribute.name, field_value.filter(pl.col("active") == 1))

        # Get all unique active concepts
        concepts_active = pl.concat([self.concept_int["id"], self.concept_no["id"]]).unique(maintain_order=False)
        logger.info("%r active concepts in INT and NO release packages combined", len(concepts_active))

        concepts_with_no_description = self.description_no_no.filter(pl.col("active") == 1)["conceptId"].unique(
            maintain_order=False,
        )
        logger.info("%r concepts with NO description", len(concepts_with_no_description))

        concept_ids = concepts_with_no_description.filter(concepts_with_no_description.is_in(concepts_active)).sort()
        logger.info("%r active concepts with NO description", len(concept_ids))

        # Filter using id_filter
        if self.config.extras.id_filter:
            logger.warning("Filtering concepts by ID")
            self.ids = concept_ids.filter(concept_ids.is_in(self.config.extras.id_filter))
        else:
            self.ids = concept_ids

        # Filter using limit
        if self.config.limit:
            logger.warning("Filtering concept IDs by LIMIT: %s", self.config.limit)
            self.ids = self.ids[: self.config.limit]

        # Reduce dataset by concept_id
        logger.debug("Filter tables based on filtered IDs")
        if self.config.limit or self.config.extras.id_filter:
            self.description_no_no = filter_table(self.description_no_no, "conceptId", self.ids)
        self.description_en_no = filter_table(self.description_en_no, "conceptId", self.ids)
        self.description_en_int = filter_table(self.description_en_int, "conceptId", self.ids)
        self.definition_no_no = filter_table(self.definition_no_no, "conceptId", self.ids)
        self.definition_en_no = filter_table(self.definition_en_no, "conceptId", self.ids)
        self.definition_en_int = filter_table(self.definition_en_int, "conceptId", self.ids)

        # Filter NO language refsets by descriptions and definitions IDs
        no_no_component_ids = pl.concat([self.description_no_no["id"], self.definition_no_no["id"]])
        self.language_nb_no = filter_table(self.language_nb_no, "referencedComponentId", no_no_component_ids)
        self.language_nb_gp_no = filter_table(self.language_nb_gp_no, "referencedComponentId", no_no_component_ids)
        self.language_nn_no = filter_table(self.language_nn_no, "referencedComponentId", no_no_component_ids)
        self.language_nn_gp_no = filter_table(self.language_nn_gp_no, "referencedComponentId", no_no_component_ids)

        # Filter EN NO language refset by descriptions and definitions IDs
        en_no_component_ids = pl.concat([self.description_en_no["id"], self.definition_en_no["id"]])
        self.language_en_no = filter_table(self.language_en_no, "referencedComponentId", en_no_component_ids)

        # Filter EN INT language reset by descriptions and definitions IDs
        en_int_component_ids = pl.concat([self.description_en_int["id"], self.definition_en_int["id"]])
        self.language_en_int = filter_table(self.language_en_int, "referencedComponentId", en_int_component_ids)

        # Filter relationship refset for where both the source and destination concept is an active concept
        self.relationship_no = self.relationship_no.filter(
            self.relationship_no["sourceId"].is_in(self.ids) | self.relationship_no["destinationId"].is_in(self.ids),
        )
        self.relationship_int = self.relationship_int.filter(
            self.relationship_int["sourceId"].is_in(self.ids) | self.relationship_int["destinationId"].is_in(self.ids),
        )
