import polars as pl

from cdef_cohort_generation.logging_config import log


def identify_events(df: pl.LazyFrame, event_definitions: dict[str, pl.Expr]) -> pl.LazyFrame:
    """Identify events based on provided definitions.

    Args:
    df (pl.LazyFrame): Input dataframe
    event_definitions (dict): Dictionary of event names and their polars expressions

    Returns:
    pl.LazyFrame: Dataframe with identified events

    """
    events = []
    for event_name, event_expr in event_definitions.items():
        try:
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
        except Exception as e:
            log(f"Warning: Error processing event '{event_name}': {str(e)}")

    if not events:
        log("Warning: No events could be identified")
        return pl.DataFrame(
            schema={"event_type": pl.Utf8, "PNR": pl.Utf8, "event_year": pl.Int64}
        ).lazy()

    return pl.concat(events)
