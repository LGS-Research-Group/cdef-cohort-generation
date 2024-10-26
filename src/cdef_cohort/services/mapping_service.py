import json
from pathlib import Path

import polars as pl

from .base import BaseService


class MappingService(BaseService):
    def __init__(self, mapping_dir: Path):
        self._mappings = {}
        self._mapping_dir = mapping_dir

    def initialize(self) -> None:
        """Initialize service resources"""
        if not self._mapping_dir.exists():
            raise ValueError(f"Mapping directory does not exist: {self._mapping_dir}")

    def shutdown(self) -> None:
        """Clean up service resources"""
        self._mappings.clear()

    def check_valid(self) -> bool:
        """Validate service configuration"""
        return self._mapping_dir.exists()

    def apply_mapping(
        self,
        col: pl.Expr,
        mapping_name: str,
        return_dtype: type[pl.DataType] = pl.Categorical
    ) -> pl.Expr:
        if mapping_name not in self._mappings:
            self.load_mapping(mapping_name)
        mapping = self._mappings[mapping_name]
        return col.map_elements(
            lambda x: mapping.get(str(x), x),
            return_dtype=return_dtype
        )

    def load_mapping(self, mapping_name: str) -> None:
        """Load mapping from JSON file"""
        mapping_file = self._mapping_dir / f"{mapping_name}.json"
        if not mapping_file.exists():
            raise FileNotFoundError(f"Mapping file not found: {mapping_file}")

        with open(mapping_file) as f:
            self._mappings[mapping_name] = json.load(f)
