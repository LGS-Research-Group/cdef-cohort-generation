import polars as pl
import polars.selectors as cs

from cdef_cohort_builder.logging_config import log, logger


def identify_events(df: pl.LazyFrame, event_definitions: dict[str, pl.Expr]) -> pl.LazyFrame:
    """Identify events based on provided definitions."""
    logger.debug(f"Starting event identification with {len(event_definitions)} event definitions")
    logger.debug(f"Input DataFrame schema: {df.collect_schema()}")

    events = []
    for event_name, event_expr in event_definitions.items():
        logger.debug(f"Processing event: {event_name}")
        try:
            # Ensure PNR and year columns are present
            required_cols = cs.by_name(["PNR", "year"])
            if not cs.expand_selector(df, required_cols):
                raise ValueError("Required columns 'PNR' and 'year' not found in DataFrame")

            event = (
                df.select(
                    pl.lit(event_name).alias("event_type"),
                    "PNR",
                    "year",
                    pl.when(event_expr).then(True).otherwise(False).alias("event_occurred"),
                )
                .filter(pl.col("event_occurred"))
                .select("event_type", "PNR", pl.col("year").alias("event_year"))
            )
            events.append(event)
            logger.debug(f"Successfully processed event: {event_name}")
            logger.debug(f"Event schema: {event.collect_schema()}")
        except Exception as e:
            log(f"Warning: Error processing event '{event_name}': {str(e)}")
            logger.debug(f"Error details for event '{event_name}': {str(e)}", exc_info=True)

    if not events:
        log("Warning: No events could be identified")
        logger.debug("Returning empty DataFrame as no events were identified")
        return pl.DataFrame(
            schema={"event_type": pl.Utf8, "PNR": pl.Utf8, "event_year": pl.Int64}
        ).lazy()

    result = pl.concat(events)
    logger.debug(f"Final concatenated events schema: {result.collect_schema()}")
    logger.debug(f"Total number of identified events: {result.collect().shape[0]}")

    return result

def test_event_identification(
    sample_df: pl.DataFrame, sample_event_definitions: dict[str, pl.Expr]
) -> None:
    logger.debug("Starting test event identification")
    logger.debug(f"Sample DataFrame: \n{sample_df}")
    logger.debug(f"Sample event definitions: {sample_event_definitions}")

    result = identify_events(sample_df.lazy(), sample_event_definitions)
    logger.debug(f"Test result: \n{result.collect()}")

# Example usage of test function
if __name__ == "__main__":
    sample_df = pl.DataFrame(
        {"PNR": ["1", "2", "3"], "year": [2020, 2021, 2022], "value": [10, 20, 30]}
    )
    sample_event_definitions = {"value_over_15": pl.col("value") > 15}
    test_event_identification(sample_df, sample_event_definitions)
