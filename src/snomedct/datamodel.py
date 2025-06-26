"""Data model definitions using attrs annotated classes.

Contains two groups:

Metadata in relation to data processing:
- Metadata
- Config
- Dataset
"""

from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

from attrs import Factory, define, field

from snomedct.logging_config import get_logger

logger = get_logger(__name__)


@define(kw_only=True, frozen=True)
class Metadata:
    """Metadata in relation to datasets.

    fields based on dcterms.
    """

    title: str | None = field(default=None)
    description: str | None = field(default=None)
    date: str = field(default=datetime.now(tz=ZoneInfo("Europe/Oslo")).isoformat())
    type_: type = field(default=None)
    source: str | None = field(default=None)
    provenance: Optional["Metadata"] = field(default=None)


@define(kw_only=True, frozen=True)
class Config:
    """Config in relation to dataset processing."""

    limit: int | None = field(default=None)
    validate: bool = field(default=True)
    safe_validation: bool = field(default=True)
    extras: Any = field(default=None)


@define(kw_only=True, frozen=True)
class Dataset:
    """Dataset that is being processed in a pipeline."""

    metadata: Metadata = field()
    config: Config = field()
    data: Any = field()
