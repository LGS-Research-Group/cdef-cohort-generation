from collections.abc import Mapping
from pathlib import Path
from typing import Any

import polars as pl

from cdef_cohort_builder.logging_config import logger
from cdef_cohort_builder.utils.columns import validate_and_select_columns
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
    """Process register data, join with population data, and save the result."""
    logger.debug(
        f"Starting process_register_data with "
        f"input_files: {input_files}, "
        f"output_file: {output_file}"
    )
    logger.debug(f"Schema: {schema}")
    logger.debug(f"Defaults: {defaults}")
    logger.debug(f"Additional kwargs: {kwargs}")

    # Merge defaults with provided kwargs, prioritizing kwargs
    params = {**defaults, **kwargs}
    logger.debug(f"Merged parameters: {params}")

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
    input_path = Path(input_files)
    if input_path.is_dir():
        file_pattern = input_path / "*.parquet"
    else:
        file_pattern = input_path

    files = list(file_pattern.parent.glob(file_pattern.name))
    logger.debug(f"Found files: {files}")

    if not files:
        logger.error(f"No parquet files found matching pattern: {file_pattern}")
        logger.debug(f"Directory contents: {list(file_pattern.parent.iterdir())}")
        raise FileNotFoundError(f"No parquet files found matching pattern: {file_pattern}")

    logger.info(f"Found {len(files)} parquet files")

    if longitudinal:
        logger.debug("Processing in longitudinal mode")
        data_frames = []
        for file in files:
            logger.debug(f"Processing file: {file.name}")
            df = pl.scan_parquet(file, allow_missing_columns=True)
            logger.debug(f"Initial schema for {file.name}: {df.collect_schema()}")

            # Add year and month columns
            date_info = extract_date_from_filename(Path(file).stem)
            logger.debug(f"Extracted date info: {date_info}")
            if "year" in date_info:
                df = df.with_columns(pl.lit(date_info["year"]).alias("year"))
            if "month" in date_info:
                df = df.with_columns(pl.lit(date_info["month"]).alias("month"))

            if columns_to_keep is not None:
                logger.debug(f"Validating and selecting columns: {columns_to_keep}")
                valid_columns, df = validate_and_select_columns(
                    df, columns_to_keep, select_columns=True
                )
                logger.debug(f"Valid columns after selection: {valid_columns}")

            data_frames.append(df)
            logger.debug(f"Added dataframe for {file.name} to data_frames")

        if not data_frames:
            logger.error("No data frames were created from the parquet files")
            raise ValueError("No data frames were created from the parquet files")

        data = pl.concat(data_frames)
        logger.debug(f"Concatenated data schema: {data.collect_schema()}")
    else:
        logger.debug("Processing in non-longitudinal mode")
        data = pl.scan_parquet(files, allow_missing_columns=True)
        logger.debug(f"Initial data schema: {data.collect_schema()}")

        if columns_to_keep is not None:
            logger.debug(f"Validating and selecting columns: {columns_to_keep}")
            valid_columns, data = validate_and_select_columns(
                data, columns_to_keep, select_columns=True
            )
            logger.debug(f"Valid columns after selection: {valid_columns}")

    # Parse date columns if specified
    if date_columns:
        logger.debug(f"Parsing date columns: {date_columns}")
        for col in date_columns:
            if col in data.collect_schema().names():
                data = data.with_columns(parse_dates(col).alias(col))
        logger.debug(f"Schema after date parsing: {data.collect_schema()}")

    # Special handling for UDDF register
    if register_name.lower() == "uddf":
        logger.debug("Performing special handling for UDDF register")
        isced_data = read_isced_data()
        data = data.join(isced_data, left_on="HFAUDD", right_on="HFAUDD", how="left")
        logger.debug(f"Schema after ISCED join: {data.collect_schema()}")

    # If population_file is None, skip joining and use the processed data as the result
    if population_file is None:
        logger.debug("No population file provided, using processed data as result")
        result = data
    else:
        logger.debug(f"Joining with population file: {population_file}")
        population = pl.scan_parquet(population_file)
        result = population

        if join_parents_only:
            logger.debug("Joining parent data only")
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
            logger.debug(f"Schema after father join: {result.collect_schema()}")

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
            logger.debug(f"Schema after mother join: {result.collect_schema()}")
        else:
            logger.debug(f"Joining on columns: {join_on}")
            join_columns = [join_on] if isinstance(join_on, str) else join_on
            result = result.join(data, on=join_columns, how="left")
            logger.debug(f"Schema after join: {result.collect_schema()}")

    # Ensure the output directory exists
    output_dir = Path(output_file).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Ensured output directory exists: {output_dir}")

    # Collect and save the result
    result.collect().write_parquet(output_file)
    logger.debug(f"Result written to: {output_file}")

    logger.info(f"Processed {register_name} data and saved to {output_file}")
