import re
from pathlib import Path
from typing import Any

import polars as pl

from cdef_cohort.logging_config import log_dataframe_info, logger
from cdef_cohort.models.config import RegisterConfig
from cdef_cohort.services.base import ConfigurableService
from cdef_cohort.services.config_service import ConfigService
from cdef_cohort.services.data_service import DataService


class RegisterService(ConfigurableService):
    def __init__(self, data_service: DataService, config_service: ConfigService):
        self.data_service = data_service
        self.config_service = config_service
        self._registers: dict[str, RegisterConfig] = {}
        self._cached_data: dict[str, pl.LazyFrame] = {}

    def configure(self, config: dict[str, Any]) -> None:
        """Configure register service with register definitions"""
        self._registers = {name: RegisterConfig(**cfg) for name, cfg in config.get("registers", {}).items()}

    def process_register_data(self, register_name: str, cache: bool = True) -> pl.LazyFrame:
        """Process data for a specific register"""
        if register_name in self._cached_data and cache:
            return self._cached_data[register_name]

        config = self._registers.get(register_name)
        if not config:
            raise ValueError(f"No configuration found for register: {register_name}")

        logger.info(f"Processing register: {register_name}")

        try:
            # Get list of input files
            input_files = self._get_input_files(config.input_files)
            if not input_files:
                raise ValueError(f"No input files found for register: {register_name}")

            logger.debug(f"Found {len(input_files)} files to process")

            # Process longitudinal data
            df = self.process_longitudinal_data(input_files, config.defaults.get("columns_to_keep"))

            # Log DataFrame info before schema application
            logger.debug("DataFrame before schema application:")
            log_dataframe_info(df, f"{register_name}_before_schema")

            # Apply schema
            df = self._apply_schema(df, config.schema_def, register_name)

            # Apply default transformations
            df = self._apply_defaults(df, config.defaults)

            # Log DataFrame info after transformations
            logger.debug("DataFrame after transformations:")
            log_dataframe_info(df, f"{register_name}_after_transform")

            if cache:
                self._cached_data[register_name] = df

            return df

        except Exception as e:
            logger.error(f"Error processing register {register_name}: {str(e)}")
            raise

    def _get_input_files(self, input_files: str | list[str]) -> list[Path]:
        """Convert input_files specification to list of Path objects"""
        if isinstance(input_files, str):
            input_path = Path(input_files)
            return list(input_path.parent.glob(input_path.name))
        return [Path(f) for f in input_files]

    def process_longitudinal_data(self, files: list[Path], columns_to_keep: list[str] | None) -> pl.LazyFrame:
        """Process longitudinal data from multiple files."""
        if not files:
            raise ValueError("No files to process")

        data_frames = []
        for file in files:
            try:
                df = pl.scan_parquet(file, allow_missing_columns=True)
                date_info = self.extract_date_from_filename(file.stem)

                if "year" in date_info:
                    df = df.with_columns(pl.lit(date_info["year"]).alias("year"))
                if "month" in date_info:
                    df = df.with_columns(pl.lit(date_info["month"]).alias("month"))

                if columns_to_keep:
                    valid_columns, df = self.validate_and_select_columns(df, columns_to_keep)
                    if not valid_columns:
                        logger.warning(f"Skipping file {file} - no valid columns found")
                        continue

                data_frames.append(df)

            except Exception as e:
                logger.error(f"Error processing file {file}: {str(e)}")
                continue

        if not data_frames:
            raise ValueError("No valid data frames to process")

        return pl.concat(data_frames)

    def extract_date_from_filename(self, filename: str) -> dict[str, int]:
        """Extract year and month (if present) from a filename."""
        logger.debug(f"Attempting to extract date from filename: {filename}")

        # Try to match YYYYMM format first
        match = re.search(r"(\d{4})(\d{2})", filename)
        if match:
            result = {"year": int(match.group(1)), "month": int(match.group(2))}
            logger.debug(f"Extracted year and month: {result}")
            return result

        # If not, try to match just YYYY
        match = re.search(r"(\d{4})", filename)
        if match:
            result = {"year": int(match.group(1))}
            logger.debug(f"Extracted year only: {result}")
            return result

        # If no match found, return an empty dict
        logger.debug("No date information found in filename")
        return {}

    def validate_and_select_columns(
        self, df: pl.LazyFrame, columns: list[str], select_columns: bool = True
    ) -> tuple[list[str], pl.LazyFrame]:
        """Validate and optionally select columns from DataFrame."""
        available_columns = set(df.collect_schema())
        valid_columns = [col for col in columns if col in available_columns]

        if len(valid_columns) < len(columns):
            missing = set(columns) - set(valid_columns)
            logger.warning(f"Missing columns: {missing}")
            if not valid_columns:
                logger.warning("No valid columns found - returning original DataFrame")
                return [], df

        if select_columns and valid_columns:
            df = df.select(valid_columns)

        return valid_columns, df

    def _apply_schema(self, df: pl.LazyFrame, schema_def: dict[str, str], register_name: str) -> pl.LazyFrame:
        """Apply schema definition to DataFrame"""
        available_columns = set(df.collect_schema().names())
        cast_expr = []

        # Get columns configuration from defaults
        defaults = self._registers[register_name].defaults
        columns_to_keep = set(defaults.get("columns_to_keep", []))
        columns_to_drop = set(defaults.get("columns_to_drop", []))

        # Validate that we're not using both columns_to_keep and columns_to_drop
        if columns_to_keep and columns_to_drop:
            raise ValueError(
                f"Cannot specify both columns_to_keep and columns_to_drop for register {register_name}. "
                "Please use only one of these options."
            )

        # Determine final set of columns to process
        if columns_to_keep:
            columns_to_process = columns_to_keep
            logger.info(f"Selected columns to keep in {register_name}: {sorted(columns_to_process)}")
        elif columns_to_drop:
            columns_to_process = available_columns - columns_to_drop
            logger.info(f"Dropping columns in {register_name}: {sorted(columns_to_drop)}")
            logger.info(f"Remaining columns to process: {sorted(columns_to_process)}")
        else:
            # If neither is specified, use all available columns
            columns_to_process = available_columns
            logger.info(f"Processing all available columns in {register_name}: {sorted(columns_to_process)}")

        # Process columns
        for col, dtype in schema_def.items():
            if col not in columns_to_process:
                continue

            if col not in available_columns:
                logger.warning(f"Column {col} not found in data - skipping")
                continue

            try:
                data_type = self._get_polars_dtype(dtype)
                cast_expr.append(pl.col(col).cast(data_type))
            except ValueError as e:
                logger.warning(f"Could not cast column {col}: {str(e)}")
                cast_expr.append(pl.col(col))

        if not cast_expr:
            return df

        # Add log message about actual columns being processed
        processed_columns = set(col.meta.output_name() for col in cast_expr)
        logger.info(f"Actually processing columns in {register_name}: {sorted(processed_columns)}")

        return df.with_columns(cast_expr)

    def _get_polars_dtype(self, dtype_str: str) -> pl.DataType:
        """Convert string dtype to polars DataType"""
        dtype_mapping = {
            "int8": pl.Int8,
            "int16": pl.Int16,
            "int32": pl.Int32,
            "int64": pl.Int64,
            "float32": pl.Float32,
            "float64": pl.Float64,
            "string": pl.Utf8,
            "bool": pl.Boolean,
            "date": pl.Date,
            "datetime": pl.Datetime,
            "category": pl.Categorical,
        }

        if dtype_str.lower() not in dtype_mapping:
            raise ValueError(f"Unsupported data type: {dtype_str}")

        return dtype_mapping[dtype_str.lower()]

    def _apply_defaults(self, df: pl.LazyFrame, defaults: dict[str, Any]) -> pl.LazyFrame:
        """Apply default transformations"""
        if cols_to_keep := defaults.get("columns_to_keep"):
            df = df.select(cols_to_keep)

        if defaults.get("longitudinal", False):
            df = self._prepare_longitudinal_data(df, defaults.get("temporal_key", "year"))

        return df

    def _prepare_longitudinal_data(self, df: pl.LazyFrame, temporal_key: str) -> pl.LazyFrame:
        """Prepare longitudinal data structure"""
        return df.sort([temporal_key, "PNR"])

    def initialize(self) -> None:
        """Initialize service resources"""
        if not self.check_valid():
            raise ValueError("Invalid configuration")

    def shutdown(self) -> None:
        """Clean up service resources"""
        self._cached_data.clear()

    def check_valid(self) -> bool:
        """Validate service configuration"""
        return True
