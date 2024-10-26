from pathlib import Path

import polars as pl

from .base import BaseService


class DataService(BaseService):
    def __init__(self):
        self._cache = {}

    def initialize(self) -> None:
        """Initialize service resources"""
        pass

    def shutdown(self) -> None:
        """Clean up service resources"""
        self._cache.clear()

    def check_valid(self) -> bool:
        """Validate service configuration"""
        return True

    def read_parquet(self, path: Path) -> pl.LazyFrame:
        """Read parquet file(s) into LazyFrame"""
        return pl.scan_parquet(path)

    def write_parquet(self, df: pl.LazyFrame, path: Path) -> None:
        """Write LazyFrame to parquet file"""
        df.collect().write_parquet(path)

    def validate_schema(self, df: pl.LazyFrame, expected_schema: dict) -> bool:
        """Validate DataFrame schema matches expected schema"""
        schema = df.collect_schema()
        return all(col in schema for col in expected_schema)
