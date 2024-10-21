import importlib.resources as pkg_resources
import json
from pathlib import Path
from typing import Any

import polars as pl

# Global dictionary to store all mappings
MAPPINGS: dict[str, dict[str, Any]] = {}

def get_mapping_path(filename: str) -> Path:
    """Get the path to a mapping file."""
    with pkg_resources.as_file(
        pkg_resources.files("cdef_cohort_builder").joinpath("mappings", filename)
    ) as path:
        return Path(path)

def load_mapping(mapping_name: str) -> None:
    """Load a mapping from a JSON file and store it in the MAPPINGS dictionary."""
    mapping_file = get_mapping_path(f"{mapping_name}.json")
    with open(mapping_file) as f:
        MAPPINGS[mapping_name] = json.load(f)

def get_mapped_value(mapping_name: str, value: Any) -> Any:
    """Get the mapped value for a given input."""
    if mapping_name not in MAPPINGS:
        load_mapping(mapping_name)
    return MAPPINGS[mapping_name].get(str(value), value)

def apply_mapping(col: pl.Expr, mapping_name: str) -> pl.Expr:
    """Apply a mapping to a Polars column expression."""
    return col.map_elements(lambda x: get_mapped_value(mapping_name, x))
