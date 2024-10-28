from pathlib import Path

import polars as pl

from .base import BaseService


class DataService(BaseService):
    def __init__(self):
        self._cache = {}
        # Initialize with string cache enabled
        self._string_cache = pl.StringCache()
        self._string_cache.__enter__()

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

    def write_parquet(
        self,
        df: pl.LazyFrame,
        path: Path,
        partition_by: str | list[str] | None = None,
    ) -> None:
        """Write LazyFrame to parquet file with optional partitioning.

        Args:
            df: LazyFrame to write
            path: Output path
            partition_by: Column(s) to partition by
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        df.collect().write_parquet(path, compression="snappy", partition_by=partition_by)

    def validate_schema(self, df: pl.LazyFrame, expected_schema: dict) -> bool:
        """Validate DataFrame schema matches expected schema"""
        schema = df.collect_schema()
        return all(col in schema for col in expected_schema)
