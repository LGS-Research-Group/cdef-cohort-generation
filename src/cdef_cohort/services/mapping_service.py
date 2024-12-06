import json
from pathlib import Path
from typing import Any

import polars as pl

from cdef_cohort.logging_config import logger

from .base import ConfigurableService


class MappingService(ConfigurableService):
    def __init__(self, mapping_dir: Path):
        self._mappings: dict[str, dict[str, Any]] = {}
        self._mapping_dir = mapping_dir
        self._isced_map: dict[str, str] | None = None

    def initialize(self) -> None:
        """Initialize service resources"""
        if not self._mapping_dir.exists():
            raise ValueError(f"Mapping directory does not exist: {self._mapping_dir}")
        self._load_isced_mapping()

    def configure(self, config: dict[str, Any]) -> None:
        """Configure the mapping service."""
        if "mapping_dir" in config:
            self._mapping_dir = Path(config["mapping_dir"])
        self._load_isced_mapping()

    def shutdown(self) -> None:
        """Clean up service resources"""
        self._mappings.clear()
        self._isced_map = None

    def check_valid(self) -> bool:
        """Validate service configuration"""
        return self._mapping_dir.exists()

    def _load_isced_mapping(self) -> None:
        """Load ISCED mapping from JSON file"""
        isced_file = self._mapping_dir / "isced.json"
        if not isced_file.exists():
            logger.warning(f"ISCED mapping file not found: {isced_file}")
            return

        try:
            with open(isced_file) as f:
                self._isced_map = json.load(f)
            logger.info("ISCED mapping loaded successfully")
        except Exception as e:
            logger.error(f"Error loading ISCED mapping: {e}")
            raise

    def apply_isced_mapping(self, df: pl.LazyFrame, column: str = "HFAUDD") -> pl.LazyFrame:
        """Apply ISCED mapping to education codes"""
        if not self._isced_map:
            logger.warning("ISCED mapping not loaded - returning original DataFrame")
            return df

        isced_map = self._isced_map  # Create local reference

        return df.with_columns(
            [
                pl.when(pl.col(column).is_not_null())
                .then(pl.col(column).map_elements(lambda x: isced_map.get(str(x), x) if isced_map else x))
                .otherwise(None)
                .alias("EDU_LVL")
            ]
        )

    def apply_mapping(
        self, col: pl.Expr, mapping_name: str, return_dtype: type[pl.DataType] = pl.Categorical
    ) -> pl.Expr:
        """Apply a named mapping to a column"""
        if mapping_name not in self._mappings:
            self.load_mapping(mapping_name)
        mapping = self._mappings[mapping_name]
        return col.map_elements(lambda x: mapping.get(str(x), x), return_dtype=return_dtype)

    def load_mapping(self, mapping_name: str) -> None:
        """Load mapping from JSON file"""
        mapping_file = self._mapping_dir / f"{mapping_name}.json"
        if not mapping_file.exists():
            raise FileNotFoundError(f"Mapping file not found: {mapping_file}")

        with open(mapping_file) as f:
            self._mappings[mapping_name] = json.load(f)
