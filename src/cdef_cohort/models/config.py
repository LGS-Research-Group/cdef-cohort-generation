from pathlib import Path
from typing import Any

import polars as pl
from pydantic import BaseModel, Field, validator


class DataDomain(BaseModel):
    sources: list[str]
    temporal: bool = True


class AnalyticalDataConfig(BaseModel):
    base_path: str
    domains: dict[str, DataDomain]


class RegisterConfig(BaseModel):
    name: str
    input_files: str | list[str]
    output_file: str
    schema_def: dict[str, str]
    defaults: dict[str, Any]
    temporal_key: str | None = None
    id_column: str = "PNR"

    class Config:
        arbitrary_types_allowed = True

    @validator("schema_def", pre=True)
    def convert_schema_types(cls, v: Any) -> dict[str, str]:
        """Convert Polars DataTypes to string representations"""
        if not isinstance(v, dict):
            return v

        def get_type_str(dtype: Any) -> str:
            if isinstance(dtype, str):
                return dtype

            # Define type mapping
            type_mapping: dict[type[pl.DataType], str] = {
                pl.Int8: "int8",
                pl.Int32: "int32",
                pl.Int64: "int64",
                pl.Float32: "float32",
                pl.Float64: "float64",
                pl.Utf8: "string",
                pl.Boolean: "bool",
                pl.Date: "date",
                pl.Datetime: "datetime",
                pl.Categorical: "category",
            }

            # Handle Polars DataTypes
            if isinstance(dtype, type) and issubclass(dtype, pl.DataType):
                # Use dict.get with a default value that calls str on the class name
                return type_mapping.get(dtype) or str(dtype.__name__)
            # Handle instance of DataType
            elif isinstance(dtype, pl.DataType):
                return str(dtype)
            return str(dtype)

        return {k: get_type_str(v[k]) for k in v}

    @validator("input_files", pre=True)
    def convert_input_files(cls, v: Any) -> str | list[str]:
        """Convert Path objects to strings"""
        if isinstance(v, Path):
            return str(v)
        elif isinstance(v, list):
            return [str(x) if isinstance(x, Path) else x for x in v]
        return v


class EventDefinition(BaseModel):
    name: str
    conditions: list[str]
    temporal_constraints: dict[str, Any] = Field(default_factory=dict)
    description: str | None = None


class PipelineStageConfig(BaseModel):
    name: str
    depends_on: list[str] = Field(default_factory=list)
    register_name: str | None = None
    output_file: str | None = None  # Changed from Path to str

    @validator("output_file", pre=True)
    def convert_output_path(cls, v):
        return str(v) if isinstance(v, Path) else v


class OutputConfig(BaseModel):
    final_cohort: str
    analytical_data: AnalyticalDataConfig


class PipelineConfig(BaseModel):
    stage_order: list[str]
    register_configs: dict[str, RegisterConfig]
    output_configs: OutputConfig  # Changed from Path to str
    stage_configs: dict[str, PipelineStageConfig] = Field(default_factory=dict)

    class Config:
        arbitrary_types_allowed = True

    @validator("output_configs", pre=True)
    def convert_output_paths(cls, v):
        return {k: str(p) if isinstance(p, Path) else p for k, p in v.items()}
