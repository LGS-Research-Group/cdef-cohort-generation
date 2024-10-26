import polars as pl
import polars.selectors as cs
from rich.markup import escape

from cdef_cohort.logging_config import logger


def validate_columns(df: pl.LazyFrame, columns: list[str]) -> bool:
    """
    Validate if all required columns are present in the DataFrame.

    Args:
        df (pl.LazyFrame): The input DataFrame to validate.
        columns (list[str]): List of column names to check for.

    Returns:
        bool: True if all columns are present, False otherwise.
    """
    schema = df.collect_schema()
    missing_columns = [col for col in columns if col not in schema]
    if missing_columns:
        logger.warning(f"Missing columns: {', '.join(missing_columns)}")
        return False
    return True


def get_event_columns(event_type: str) -> tuple[str, str]:
    """Get the correct year and PNR columns based on the event type."""
    if event_type == "child":
        return "year", "PNR"
    elif event_type == "father":
        return "FAR_year", "FATHER_PNR"
    elif event_type == "mother":
        return "MOR_year", "MOTHER_PNR"
    else:
        raise ValueError(f"Invalid event_type: {event_type}")


def identify_events(
    df: pl.LazyFrame, event_definitions: dict[str, pl.Expr], event_type: str
) -> pl.LazyFrame:
    """
    Identify events in the input DataFrame based on the provided event definitions.

    Args:
        df (pl.LazyFrame): The input DataFrame to process.
        event_definitions (dict[str, pl.Expr]):
            A dictionary of event names and their corresponding Polars expressions.
        event_type (str): The type of event (child, father, or mother).

    Returns:
        pl.LazyFrame: A LazyFrame containing identified events with columns
        'event_type', 'PNR', and 'year'.
    """
    logger.info(f"Starting event identification with {len(event_definitions)} event definitions")
    logger.debug(f"Input DataFrame schema: {df.collect_schema()}")

    events = []
    for event_name, event_expr in event_definitions.items():
        logger.info(f"Processing event: {event_name}")

        try:
            # Determine the correct year and PNR columns based on the event type
            year_col, pnr_col = get_event_columns(event_type)

            # Validate columns
            columns_used = event_expr.meta.root_names()
            if not validate_columns(df, columns_used):
                logger.warning(f"Skipping event '{event_name}' due to missing columns")
                continue

            # Include categorical columns in the event data
            categorical_cols = df.select(cs.categorical()).columns
            event = (
                df.select(
                    pl.lit(event_name).alias("event_type"),
                    pl.col(pnr_col).alias("PNR"),
                    pl.col(year_col).alias("year"),
                    pl.when(event_expr).then(True).otherwise(False).alias("event_occurred"),
                    *[pl.col(col) for col in categorical_cols],
                )
                .filter(pl.col("event_occurred"))
                .select(["event_type", "PNR", "year"] + categorical_cols)
            )

            # Collect the event data
            event_details = event.collect()

            # Log event details without using markup
            logger.info(f"Event: {event_name}")
            logger.info(f"Number of occurrences: {event_details.shape[0]}")
            logger.info(f"Sample of identified events:\n{event_details.head()}")

            events.append(event)
            logger.debug(f"Successfully processed event: {event_name}")
        except Exception as e:
            # Escape the error message to prevent markup interpretation
            error_msg = escape(str(e))
            logger.error(f"Error processing event '{event_name}': {error_msg}", exc_info=True)

    if not events:
        logger.warning("No events could be identified")
        return pl.DataFrame(schema={"event_type": pl.Utf8, "PNR": pl.Utf8, "year": pl.Int64}).lazy()

    result = pl.concat(events)
    logger.debug(f"Total number of identified events: {result.collect().shape[0]}")
    return result


def test_event_identification(
    sample_df: pl.DataFrame, sample_event_definitions: dict[str, pl.Expr]
) -> None:
    """
    Test the event identification function with sample data.

    Args:
        sample_df (pl.DataFrame): A sample DataFrame for testing.
        sample_event_definitions (dict[str, pl.Expr]): Sample event definitions for testing.

    Returns:
        None
    """
    logger.debug("Starting test event identification")
    logger.debug(f"Sample DataFrame: \n{sample_df}")
    logger.debug(f"Sample event definitions: {sample_event_definitions}")

    result = identify_events(sample_df.lazy(), sample_event_definitions, "child")
    logger.debug(f"Test result: \n{result.collect()}")


# Example usage of test function
if __name__ == "__main__":
    sample_df = pl.DataFrame(
        {"PNR": ["1", "2", "3"], "year": [2020, 2021, 2022], "value": [10, 20, 30]}
    )
    sample_event_definitions = {"value_over_15": pl.col("value") > 15}
    test_event_identification(sample_df, sample_event_definitions)
