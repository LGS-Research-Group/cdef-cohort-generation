import glob
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import polars as pl

from cdef_cohort_builder.logging_config import logger
from cdef_cohort_builder.utils.date import extract_date_from_filename, parse_dates
from cdef_cohort_builder.utils.isced import read_isced_data
from cdef_cohort_builder.utils.types import KwargsType


def process_register_data(
    input_files: Path,
    output_file: Path,
    schema: Mapping[str, pl.DataType | type[pl.DataType]],
    defaults: dict[str, Any],
    **kwargs: KwargsType,
) -> None:
    """Process register data, join with population data, and save the result.

    Args:
    input_files (Path): Path to input parquet files.
    output_file (Path): Path to save the output parquet file.
    schema (Dict[str, pl.DataType]): Schema for the input data.
    defaults (dict[str, Any]): Default parameters for processing.
    **kwargs: Additional keyword arguments.

    Returns:
    None
    """
    # Merge defaults with provided kwargs, prioritizing kwargs
    params = {**defaults, **kwargs}

    population_file = params.get("population_file")
    date_columns = params.get("date_columns")
    columns_to_keep = params.get("columns_to_keep")
    join_on = params.get("join_on", "PNR")
    join_parents_only = params.get("join_parents_only", False)
    register_name = params.get("register_name", "")
    longitudinal = params.get("longitudinal", False)

    logger.info(f"Processing register: {register_name}")
    logger.info(f"Input files path: {input_files}")

    # Use glob to find all .parquet files
    file_pattern = str(input_files)
    if not file_pattern.endswith("*.parquet"):
        file_pattern = str(input_files / "*.parquet")
    files = glob.glob(file_pattern)

    if not files:
        logger.error(f"No parquet files found matching pattern: {file_pattern}")
        logger.info(f"Directory contents: {list(Path(input_files).parent.iterdir())}")
        raise FileNotFoundError(f"No parquet files found matching pattern: {file_pattern}")

    logger.info(f"Found {len(files)} parquet files")

    if longitudinal:
        data_frames = []
        for file in files:
            logger.info(f"Processing {file}")
            df = pl.scan_parquet(file, allow_missing_columns=True)

            # Get the columns that exist in this file
            existing_columns = set(df.collect_schema().names())

            # If columns_to_keep is specified and not None, only keep columns that exist
            if columns_to_keep is not None:
                columns_to_keep = [col for col in columns_to_keep if col in existing_columns]
                df = df.select(columns_to_keep)

            date_info = extract_date_from_filename(Path(file).stem)
            if "year" in date_info:
                df = df.with_columns(pl.lit(date_info["year"]).alias("year"))
            if "month" in date_info:
                df = df.with_columns(pl.lit(date_info["month"]).alias("month"))
            data_frames.append(df)

        if not data_frames:
            logger.error("No data frames were created from the parquet files")
            raise ValueError("No data frames were created from the parquet files")

        data = pl.concat(data_frames)
    else:
        data = pl.scan_parquet(files, allow_missing_columns=True)

        # Get the columns that exist in the data
        existing_columns = set(data.collect_schema().names())

        # If columns_to_keep is specified and not None, only keep columns that exist
        if columns_to_keep is not None:
            columns_to_keep = [col for col in columns_to_keep if col in existing_columns]
            data = data.select(columns_to_keep)

    # Parse date columns if specified
    if date_columns:
        for col in date_columns:
            if col in data.collect_schema().names():
                data = data.with_columns(parse_dates(col).alias(col))

    # Special handling for UDDF register
    if register_name.lower() == "uddf":
        isced_data = read_isced_data()
        data = data.join(isced_data, left_on="HFAUDD", right_on="HFAUDD", how="left")

    # If population_file is None, skip joining and use the processed data as the result
    if population_file is None:
        result = data
    else:
        # Read in the population file
        population = pl.scan_parquet(population_file)

        # Prepare result dataframe
        result = population

        # If joining on parents, we need to join twice more for parent-specific data
        if join_parents_only:
            # For father's data
            father_data = data.select(
                [
                    pl.col(col).alias(f"FAR_{col}" if col != join_on else col)
                    for col in data.collect_schema().names()
                ]
            )
            result = result.join(
                father_data,
                left_on="FAR_ID",
                right_on=join_on,
                how="left",
            )

            # For mother's data
            mother_data = data.select(
                [
                    pl.col(col).alias(f"MOR_{col}" if col != join_on else col)
                    for col in data.collect_schema().names()
                ]
            )
            result = result.join(
                mother_data,
                left_on="MOR_ID",
                right_on=join_on,
                how="left",
            )
        else:
            # Join on specified column(s)
            join_columns = [join_on] if isinstance(join_on, str) else join_on
            result = result.join(data, on=join_columns, how="left")

    # Ensure the output directory exists
    output_dir = Path(output_file).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    # Collect and save the result
    result.collect().write_parquet(output_file)

    logger.info(f"Processed {register_name} data and saved to {output_file}")