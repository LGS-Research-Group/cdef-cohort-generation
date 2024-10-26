from typing import Any

import polars as pl

from cdef_cohort.logging_config import logger


def validate_and_select_columns(
    data: pl.LazyFrame,
    columns_to_check: list[Any],
    select_columns: bool = False,
    always_include: tuple[str, ...] = ("year", "month"),
) -> tuple[list[str], pl.LazyFrame]:
    """
    Validates columns against the LazyFrame schema and optionally selects matching columns.
    Prints warnings for columns not found in the schema.

    Args:
    data (pl.LazyFrame):
        The input Polars LazyFrame.
    columns_to_check (List[str]):
        List of column names to check against the LazyFrame schema.
    select_columns (bool):
        If True, returns a new LazyFrame with only the matching columns.
    always_include (List[str]):
        List of columns to always include if they exist in the data.

    Returns:
    Tuple[List[str], pl.LazyFrame]:
        (list of valid columns, LazyFrame with selected columns if select_columns is True)
    """
    logger.debug("Starting validate_and_select_columns")
    logger.debug(
        f"Input parameters: columns_to_check={columns_to_check}, "
        f"select_columns={select_columns}, always_include={always_include}"
    )

    existing_columns = set(data.collect_schema().names())
    logger.debug(f"Existing columns in the LazyFrame: {existing_columns}")

    valid_columns = []

    # First, check and include columns from columns_to_check
    for col in columns_to_check:
        if col in existing_columns:
            valid_columns.append(col)
            logger.debug(f"Column '{col}' found and added to valid columns")
        else:
            logger.warning(f"Column '{col}' not found in the LazyFrame schema.")

    logger.debug(f"Valid columns after checking columns_to_check: {valid_columns}")

    # Then, include columns from always_include if they exist
    for col in always_include:
        if col in existing_columns and col not in valid_columns:
            valid_columns.append(col)
            logger.info(f"Automatically included existing column '{col}'.")
            logger.debug(f"Added '{col}' from always_include to valid columns")

    logger.debug(f"Final list of valid columns: {valid_columns}")

    if select_columns:
        logger.debug("Selecting columns from LazyFrame")
        selected_data = data.select(valid_columns)
        logger.debug(f"Selected data schema: {selected_data.collect_schema()}")
        return valid_columns, selected_data
    else:
        logger.debug("Returning original LazyFrame without selection")
        return valid_columns, data
