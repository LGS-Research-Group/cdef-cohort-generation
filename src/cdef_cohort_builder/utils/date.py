import re

import polars as pl


def parse_dates(col_name: str) -> pl.Expr:
    return pl.coalesce(
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


def extract_date_from_filename(filename: str) -> dict[str, int]:
    """Extract year and month (if present) from filename.

    Args:
    filename (str): Name of the file.

    Returns:
    dict: A dictionary with 'year' and optionally 'month' keys.

    """
    # Try to match YYYYMM format first
    match = re.search(r"(\d{4})(\d{2})", filename)
    if match:
        return {"year": int(match.group(1)), "month": int(match.group(2))}

    # If not, try to match just YYYY
    match = re.search(r"(\d{4})", filename)
    if match:
        return {"year": int(match.group(1))}

    # If no match found, return an empty dict
    return {}
