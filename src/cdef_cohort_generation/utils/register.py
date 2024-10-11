from collections.abc import Mapping
from pathlib import Path

import polars as pl
import polars.selectors as cs

from cdef_cohort_generation.utils import (
    KwargsType,
    extract_date_from_filename,
    parse_dates,
    read_isced_data,
)


def process_register_data(
    input_files: Path,
    output_file: Path,
    population_file: Path,
    schema: Mapping[str, pl.DataType | type[pl.DataType]],
    date_columns: list[str] | None = None,
    columns_to_keep: list[str] | None = None,
    join_on: str | list[str] = "PNR",
    join_parents_only: bool = False,
    register_name: str = "",
    longitudinal: bool = False,
    **kwargs: KwargsType,
) -> None:
    """Process register data, join with population data, and save the result.

    Args:
    input_files (Path): Path to input parquet files.
    output_file (Path): Path to save the output parquet file.
    population_file (Path): Path to the population parquet file.
    schema (Dict[str, pl.DataType]): Schema for the input data.
    date_columns (Optional[List[str]]): List of column names to parse as dates.
    columns_to_keep (Optional[List[str]]): List of columns to keep in the final output.
    join_on (str | List[str]): Column(s) to join on. Default is "PNR".
    join_parents_only (bool): If True, only join on FAR_ID and MOR_ID. Default is False.
    register_name (str): Name of the register being processed. Default is "".
    longitudinal (bool): If True, treat data as longitudinal
    and extract year (and month if present) from filename. Default is False.

    Returns:
    None

    """
    if longitudinal:
        data_frames = []
        for file in input_files.glob("*.parquet"):
            df = pl.scan_parquet(file)
            date_info = extract_date_from_filename(file.stem)
            if "year" in date_info:
                df = df.with_columns(pl.lit(date_info["year"]).alias("year"))
            if "month" in date_info:
                df = df.with_columns(pl.lit(date_info["month"]).alias("month"))
            data_frames.append(df)
        data = pl.concat(data_frames)
    else:
        data = pl.scan_parquet(input_files, allow_missing_columns=True)

    # Parse date columns if specified
    if date_columns:
        for col in date_columns:
            data = data.with_columns(parse_dates(col).alias(col))

    # Select specific columns if specified
    if columns_to_keep:
        data = data.select(columns_to_keep)

    # Special handling for UDDF register
    if register_name.lower() == "uddf":
        isced_data = read_isced_data()
        data = data.join(isced_data, left_on="HFAUDD", right_on="HFAUDD", how="left")

    # Read in the population file
    population = pl.scan_parquet(population_file)

    # Prepare result dataframe
    result = population

    # If joining on parents, we need to join twice more for parent-specific data
    if join_parents_only:
        result = result.join(
            data.select(cs.starts_with("FAR_")),
            left_on="FAR_ID",
            right_on=f"FAR_{join_on}",
            how="left",
        )
        result = result.join(
            data.select(cs.starts_with("MOR_")),
            left_on="MOR_ID",
            right_on=f"MOR_{join_on}",
            how="left",
        )
    else:
        # Join on specified column(s)
        join_columns = [join_on] if isinstance(join_on, str) else join_on
        result = result.join(data, on=join_columns, how="left")

    # Collect and save the result
    result.collect().write_parquet(output_file)
