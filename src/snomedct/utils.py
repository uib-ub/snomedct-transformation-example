"""General utilities."""

import inspect
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

import polars as pl
from attrs import fields

from snomedct.logging_config import get_logger

logger = get_logger(__name__)


def get_current_function_name() -> str:
    """Get current function name."""
    return str(inspect.stack()[1][3])


def attrs_to_polars_schema(cls: type) -> dict:
    """Transform attrs type annotation to polars schema."""
    if not hasattr(cls, "__attrs_attrs__"):
        logger.error("Class must be attrs decorated")
        raise TypeError

    type_map = {
        UUID: pl.Utf8,
        bool: pl.Boolean,
        bytes: pl.Binary,
        date: pl.Date,
        datetime: pl.Datetime,
        Decimal: pl.Decimal,
        float: pl.Float64,
        int: pl.Int64,
        str: pl.Utf8,
    }
    for field in fields(cls):
        if field.type not in type_map:
            logger.error("Unsupported type %r for field %r", field.type, field.name)
            raise TypeError

    return {field.name: type_map[field.type] for field in fields(cls)}
