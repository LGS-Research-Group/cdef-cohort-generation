import polars as pl


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
        event = df.filter(event_expr).select(
            pl.lit(event_name).alias("event_type"),
            pl.col("PNR"),
            pl.col("year").alias("event_year"),
        )
        events.append(event)

    return pl.concat(events)
