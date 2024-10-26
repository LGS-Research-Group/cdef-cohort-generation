from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import polars as pl
from polars.datatypes import DataTypeClass
from tqdm import tqdm

from cdef_cohort.logging_config import logger
from cdef_cohort.utils.columns import validate_and_select_columns
from cdef_cohort.utils.date import extract_date_from_filename, parse_dates
from cdef_cohort.utils.isced import read_isced_data
from cdef_cohort.utils.types import KwargsType


def process_register_data(
    input_files: Path | str,
    output_file: Path | str,
    schema: Mapping[str, DataTypeClass],
    defaults: dict[str, Any],
    preprocess_func: Callable[[pl.LazyFrame], pl.LazyFrame] | None = None,
    **kwargs: KwargsType,
) -> None:
    """Process register data, join with population data, and save the result."""
    logger.debug(
        f"Starting process_register_data with input_files: {input_files}, "
        f"output_file: {output_file}"
    )
    logger.debug(f"Schema: {schema}")
    logger.debug(f"Defaults: {defaults}")
    logger.debug(f"Additional kwargs: {kwargs}")

    params = {**defaults, **kwargs}
    logger.debug(f"Merged parameters: {params}")

    population_file: Path | str | None = params.get("population_file")
    date_columns: list[str] = params.get("date_columns", [])
    columns_to_keep: list[str] | None = params.get("columns_to_keep")
    join_on: str | list[str] = params.get("join_on", "PNR")
    join_parents_only: bool = params.get("join_parents_only", False)
    register_name: str = params.get("register_name", "")
    longitudinal: bool = params.get("longitudinal", False)

    logger.info(f"Processing register: {register_name}")
    logger.info(f"Input files path: {input_files}")

    input_path = Path(input_files)
    file_pattern = input_path / "*.parquet" if input_path.is_dir() else input_path
    files = list(file_pattern.parent.glob(file_pattern.name))

    if not files:
        logger.error(f"No parquet files found matching pattern: {file_pattern}")
        raise FileNotFoundError(f"No parquet files found matching pattern: {file_pattern}")

    logger.info(f"Found {len(files)} parquet files")

    if longitudinal:
        data = process_longitudinal_data(files, columns_to_keep)
    else:
        data = pl.scan_parquet(files, allow_missing_columns=True)
        if columns_to_keep:
            valid_columns, data = validate_and_select_columns(
                data, columns_to_keep, select_columns=True
            )

    # Parse date columns
    if date_columns:
        for col in date_columns:
            if col in data.collect_schema().names():
                data = data.with_columns(parse_dates(col).alias(col))

    # Apply preprocessing function if provided
    if preprocess_func:
        data = preprocess_func(data)

    # Special handling for UDDF register
    if register_name.lower() == "uddf":
        isced_data = read_isced_data()
        data = data.join(isced_data, left_on="HFAUDD", right_on="HFAUDD", how="left")

    # Join with population data if provided
    result = (
        join_with_population(data, population_file, join_on, join_parents_only)
        if population_file
        else data
    )

    # Ensure the output directory exists
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Collect and save the result
    result.collect().write_parquet(output_path)
    logger.info(f"Processed {register_name} data and saved to {output_file}")


def process_longitudinal_data(files: list[Path], columns_to_keep: list[str] | None) -> pl.LazyFrame:
    data_frames = []
    for file in tqdm(files, desc="Processing files"):
        df = pl.scan_parquet(file, allow_missing_columns=True)
        date_info = extract_date_from_filename(file.stem)
        if "year" in date_info:
            df = df.with_columns(pl.lit(date_info["year"]).alias("year"))
        if "month" in date_info:
            df = df.with_columns(pl.lit(date_info["month"]).alias("month"))
        if columns_to_keep:
            valid_columns, df = validate_and_select_columns(
                df, columns_to_keep, select_columns=True
            )
        data_frames.append(df)
    return pl.concat(data_frames)


def join_with_population(
    data: pl.LazyFrame,
    population_file: Path | str,
    join_on: str | list[str],
    join_parents_only: bool,
) -> pl.LazyFrame:
    population = pl.scan_parquet(population_file)
    if join_parents_only:
        result = join_parents(population, data, join_on)
    else:
        join_columns = [join_on] if isinstance(join_on, str) else join_on
        result = population.join(data, on=join_columns, how="left")
    return result


def join_parents(
    population: pl.LazyFrame, data: pl.LazyFrame, join_on: str | list[str]
) -> pl.LazyFrame:
    father_data = data.select(
        [
            pl.col(col).alias(f"FAR_{col}" if col != join_on else col)
            for col in data.collect_schema().names()
        ]
    )
    result = population.join(father_data, left_on="FAR_ID", right_on=join_on, how="left")

    mother_data = data.select(
        [
            pl.col(col).alias(f"MOR_{col}" if col != join_on else col)
            for col in data.collect_schema().names()
        ]
    )
    result = result.join(mother_data, left_on="MOR_ID", right_on=join_on, how="left")

    return result
