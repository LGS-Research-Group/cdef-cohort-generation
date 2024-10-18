import re

import polars as pl

from cdef_cohort_builder.logging_config import logger


def parse_dates(col_name: str) -> pl.Expr:
    logger.debug(f"Attempting to parse dates for column: {col_name}")

    parsed = pl.coalesce(
        # Prioritize formats with '/' separator
        pl.col(col_name).str.strptime(pl.Date, "%Y/%m/%d", strict=False),
        pl.col(col_name).str.strptime(pl.Date, "%d/%m/%Y", strict=False),
        pl.col(col_name).str.strptime(pl.Date, "%m/%d/%Y", strict=False),
        pl.col(col_name).str.strptime(pl.Date, "%Y/%m/%d %H:%M:%S", strict=False),
        pl.col(col_name).str.strptime(pl.Date, "%m/%d/%y", strict=False),
        # LPR3 format for dates
        pl.col(col_name).str.strptime(pl.Date, "%d%b%Y", strict=False),
        # Then formats with '-' separator
        pl.col(col_name).str.strptime(pl.Date, "%Y-%m-%d", strict=False),
        pl.col(col_name).str.strptime(pl.Date, "%d-%m-%Y", strict=False),
        pl.col(col_name).str.strptime(pl.Date, "%m-%d-%Y", strict=False),
        pl.col(col_name).str.strptime(pl.Date, "%Y-%m-%d %H:%M:%S", strict=False),
        # Locale's appropriate date and time representation
        pl.col(col_name).str.strptime(pl.Date, "%c", strict=False),
    )

    logger.debug(f"Finished parsing dates for column: {col_name}")
    return parsed


def extract_date_from_filename(filename: str) -> dict[str, int]:
    """Extract year and month (if present) from filename.

    Args:
    filename (str): Name of the file.

    Returns:
    dict: A dictionary with 'year' and optionally 'month' keys.

    """
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


# You might want to add a function to test the date parsing
def test_date_parsing(sample_date: str) -> None:
    logger.debug(f"Testing date parsing with sample: {sample_date}")
    df = pl.DataFrame({"sample_col": [sample_date]})
    parsed = df.with_columns(parse_dates("sample_col")).select("sample_col")
    logger.debug(f"Parsed result: {parsed[0, 0]}")


# Example usage of test function
if __name__ == "__main__":
    test_date_parsing("2023/05/15")
    test_date_parsing("15/05/2023")
    test_date_parsing("2023-05-15")
    test_date_parsing("15May2023")
