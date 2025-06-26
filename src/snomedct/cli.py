import typer
from cattrs import structure
from rich import print

from snomedct.datamodel import Config
from snomedct.snomedct import snomed_denormalize_dataset, snomed_load_dataset_from_files
from snomedct.snomedct_config import extras
from snomedct.snomedct_datamodel import SnomedExtras

app = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=False)


@app.command()
def main(
    limit: int | None = None,
    id_filter: str | None = None,  # comma separated list
    *,
    validate: bool = True,
) -> None:
    """Transform snomed data."""
    id_filter_split = tuple(id_filter.split(",")) if id_filter else None
    config = Config(
        limit=limit,
        validate=validate,
        extras=structure({**extras, "id_filter": id_filter_split}, SnomedExtras),
    )

    snomed_dataset_raw = snomed_load_dataset_from_files(config)

    snomed_dataset = snomed_denormalize_dataset(snomed_dataset_raw)
    print(snomed_dataset.metadata)
    print(snomed_dataset.data)


if __name__ == "__main__":
    app()
