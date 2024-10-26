from logging import getLogger
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # Use the 'Agg' backend which doesn't require a GUI
import polars as pl
import polars.selectors as cs

from cdef_cohort.logging_config import logger

getLogger("matplotlib.font_manager").disabled = True


def save_time_series_data(df: pl.LazyFrame, output_dir: Path) -> None:
    """
    Save time series data of event occurrences to CSV files.

    Args:
        df (pl.LazyFrame): A LazyFrame containing event data with 'year' and 'event_type' columns.
        output_dir (Path): Directory to save the CSV files.
    """
    event_counts = (
        df.with_columns(pl.col("year").cast(pl.Int32))
        .group_by(["year", "event_type"])
        .count()
        .collect()
        .pivot(
            values="count",
            index="year",
            on="event_type",
            aggregate_function="first",
        )
        .fill_null(0)
    )
    event_counts.write_csv(output_dir / "event_occurrences.csv")

    categorical_cols = df.select(cs.categorical()).columns
    for cat_col in categorical_cols:
        cat_counts = (
            df.group_by(["year", cat_col])
            .count()
            .collect()
            .pivot(values="count", index="year", on=cat_col)
            .fill_null(0)
        )
        cat_counts.write_csv(output_dir / f"{cat_col}_distribution.csv")


def save_event_heatmap_data(df: pl.LazyFrame, output_dir: Path) -> None:
    """
    Save event co-occurrence data for heatmap to a CSV file.

    Args:
        df (pl.LazyFrame): A LazyFrame containing event data with 'PNR' and 'event_type' columns.
        output_dir (Path): Directory to save the CSV file.
    """
    event_pivot = (
        df.collect()
        .pivot(
            values="year",
            index="PNR",
            on="event_type",
            aggregate_function="len",
        )
        .fill_null(0)
    )
    numeric_cols = cs.expand_selector(event_pivot, cs.numeric())
    event_pivot_numeric = event_pivot.select(numeric_cols)
    corr = event_pivot_numeric.corr()
    corr.write_csv(output_dir / "event_correlation.csv")


def save_stacked_bar_data(df: pl.LazyFrame, group_col: str, output_dir: Path) -> None:
    """
    Save data for stacked bar chart to CSV files.

    Args:
        df (pl.LazyFrame):
            A LazyFrame containing event data with 'event_type' and the specified group column.
        group_col (str): The name of the column to use for grouping.
        output_dir (Path): Directory to save the CSV files.
    """
    grouped = (
        df.group_by([group_col, "event_type"])
        .count()
        .collect()
        .pivot(
            values="count",
            index=group_col,
            on="event_type",
            aggregate_function="first",
        )
        .fill_null(0)
    )

    grouped_pct = grouped.select(
        pl.col(group_col), pl.all().exclude(group_col) / pl.all().exclude(group_col).sum()
    )
    grouped_pct.write_csv(output_dir / f"event_distribution_{group_col}.csv")

    categorical_cols = df.select(cs.categorical()).columns
    for cat_col in categorical_cols:
        cat_grouped = (
            df.group_by([group_col, cat_col])
            .count()
            .collect()
            .pivot(values="count", index=group_col, on=cat_col, aggregate_function="first")
            .fill_null(0)
        )
        cat_grouped_pct = cat_grouped.select(
            pl.col(group_col), pl.all().exclude(group_col) / pl.all().exclude(group_col).sum()
        )
        cat_grouped_pct.write_csv(output_dir / f"{cat_col}_distribution_{group_col}.csv")


def save_sankey_data(
    df: pl.LazyFrame, event_sequence: list[str], top_n: int, min_count: int, output_dir: Path
) -> None:
    """Save Sankey diagram data to a CSV file."""
    flows = (
        df.select(["PNR", "event_type", "year"])
        .rename({"year": "year"})
        .sort(["PNR", "year"])
        .group_by("PNR")
        .agg(pl.col("event_type").alias("event_sequence"))
        .select(pl.col("event_sequence").list.join("-"))
        .group_by("event_sequence")
        .count()
        .sort("count", descending=True)
        .collect()
    )

    top_events = set(
        df.group_by("event_type")
        .count()
        .sort("count", descending=True)
        .limit(top_n)
        .collect()["event_type"]
    )

    sankey_data = []

    for sequence, count in zip(flows["event_sequence"], flows["count"], strict=False):
        if count < min_count:
            continue
        events = sequence.split("-")
        for i in range(len(events) - 1):
            src = events[i] if events[i] in top_events else "Other"
            tgt = events[i + 1] if events[i + 1] in top_events else "Other"
            if src != tgt:
                sankey_data.append({"source": src, "target": tgt, "value": count})

    pl.DataFrame(sankey_data).write_csv(output_dir / "sankey_data.csv")


def save_survival_curve_data(df: pl.LazyFrame, event_type: str, output_dir: Path) -> None:
    """
    Save survival curve data to a CSV file.

    Args:
        df (pl.LazyFrame):
            A LazyFrame containing event data with 'PNR', 'event_type', and 'year' columns.
        event_type (str): The specific event type to analyze.
        output_dir (Path): Directory to save the CSV file.
    """
    try:
        event_df = df.filter(pl.col("event_type") == event_type).collect()
        min_year = df.select(pl.min("year")).collect().item()

        T = (
            event_df.group_by("PNR")
            .agg(time_to_event=(pl.col("year").min() - min_year).cast(pl.Int32), E=pl.count("PNR"))
            .drop_nulls()
        )

        if T.height == 0:
            logger.error(f"No valid data for survival analysis of {event_type}")
            return

        T.write_csv(output_dir / f"survival_data_{event_type}.csv")
        logger.info(f"Survival data for {event_type} saved successfully")
    except Exception as e:
        logger.error(f"Error in saving survival data for {event_type}: {str(e)}", exc_info=True)


def save_plot_data(df: pl.LazyFrame, output_dir: Path) -> None:
    """
    Save all plot data to CSV files.

    Args:
        df (pl.LazyFrame): A LazyFrame containing event data.
        output_dir (Path): Directory to save the CSV files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    save_time_series_data(df, output_dir)
    save_event_heatmap_data(df, output_dir)
    save_stacked_bar_data(df, "year", output_dir)  # Assuming 'year' as the group column
    save_sankey_data(
        df,
        df.select(pl.col("event_type").unique()).collect().to_series().to_list(),
        5,
        10,
        output_dir,
    )

    event_types = df.select(pl.col("event_type").unique()).collect().to_series().to_list()
    for event_type in event_types:
        save_survival_curve_data(df, event_type, output_dir)

    logger.info(f"All plot data has been saved to {output_dir}")
