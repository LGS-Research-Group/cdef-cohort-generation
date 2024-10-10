from pathlib import Path

import polars as pl


def parse_dates(col_name: str) -> pl.Expr:
    return pl.coalesce(
        # Prioritize formats with '/' separator
        pl.col(col_name).str.strptime(pl.Date, "%Y/%m/%d", strict=False),
        pl.col(col_name).str.strptime(pl.Date, "%d/%m/%Y", strict=False),
        pl.col(col_name).str.strptime(pl.Date, "%m/%d/%Y", strict=False),
        pl.col(col_name).str.strptime(pl.Date, "%Y/%m/%d %H:%M:%S", strict=False),
        pl.col(col_name).str.strptime(pl.Date, "%m/%d/%y", strict=False),
        # LPR3 Format for dates
        pl.col(col_name).str.strptime(pl.Date, "%d%b%Y", strict=False),
        # Then formats with '-' separator
        pl.col(col_name).str.strptime(pl.Date, "%Y-%m-%d", strict=False),
        pl.col(col_name).str.strptime(pl.Date, "%d-%m-%Y", strict=False),
        pl.col(col_name).str.strptime(pl.Date, "%m-%d-%Y", strict=False),
        pl.col(col_name).str.strptime(pl.Date, "%Y-%m-%d %H:%M:%S", strict=False),
        # Locale's appropriate date and time representation
        pl.col(col_name).str.strptime(pl.Date, "%c", strict=False),
    )


def process_register_data(
    input_files: Path,
    output_file: Path,
    population_file: Path,
    schema: dict[str, pl.DataType],
    date_columns: list[str] | None = None,
    columns_to_keep: list[str] | None = None,
) -> None:
    """
    Process register data, join with population data, and save the result.

    Args:
    input_files (List[Path]): List of input parquet files.
    output_file (Path): Path to save the output parquet file.
    population_file (Path): Path to the population parquet file.
    schema (Dict[str, pl.DataType]): Schema for the input data.
    date_columns (Optional[List[str]]): List of column names to parse as dates.
    columns_to_keep (Optional[List[str]]): List of columns to keep in the final output.

    Returns:
    None
    """
    # Read all input parquet files
    data = pl.scan_parquet(input_files, allow_missing_columns=True)

    # Parse date columns if specified
    if date_columns:
        for col in date_columns:
            data = data.with_columns(parse_dates(col).alias(col))

    # Select specific columns if specified
    if columns_to_keep:
        data = data.select(columns_to_keep)

    # Read in the population file
    population = pl.scan_parquet(population_file)

    # Join the data with the population file
    result = population.join(data, on="PNR", how="left").collect()

    # Save the result
    result.write_parquet(output_file)
