from logging import getLogger

import matplotlib

matplotlib.use("Agg")  # Use the 'Agg' backend which doesn't require a GUI
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import polars as pl
import polars.selectors as cs
import seaborn as sns
from lifelines import KaplanMeierFitter
from matplotlib.figure import Figure

from cdef_cohort_builder.logging_config import logger

getLogger("matplotlib.font_manager").disabled = True


def plot_time_series(df: pl.LazyFrame) -> Figure:
    """
    Create a time series plot of event occurrences.

    This function takes a LazyFrame containing event data and generates a time series plot
    showing the number of occurrences for each event type over time.

    Args:
        df (pl.LazyFrame): A LazyFrame containing event data with 'year' and 'event_type' columns.

    Returns:
        Figure: A matplotlib Figure object containing the time series plot.
    """
    event_counts = (
        df.with_columns(pl.col("year").cast(pl.Int32))  # Cast year to Int32
        .group_by(["year", "event_type"])
        .count()
        .collect()  # Collect before pivot
        .pivot(
            values="count",
            index="year",
            on="event_type",
            aggregate_function="first",
        )
        .fill_null(0)
    )

    fig, ax = plt.subplots(figsize=(14, 8))
    for column in event_counts.columns[1:]:
        ax.plot(event_counts["year"], event_counts[column], label=column, marker="o")

    ax.set_title("Event Occurrences Over Time", fontsize=16)
    ax.set_xlabel("Year", fontsize=12)
    ax.set_ylabel("Number of Occurrences", fontsize=12)
    ax.legend(bbox_to_anchor=(1.05, 1), loc="upper left", fontsize=10)
    ax.grid(True, linestyle="--", alpha=0.7)
    plt.tight_layout()
    return fig


def plot_event_heatmap(df: pl.LazyFrame) -> Figure:
    """
    Create a heatmap of event co-occurrences.

    This function generates a heatmap showing the correlation between different event types
    based on their co-occurrence in the dataset.

    Args:
        df (pl.LazyFrame): A LazyFrame containing event data with 'PNR' and 'event_type' columns.

    Returns:
        Figure: A matplotlib Figure object containing the heatmap.
    """
    # Collect and pivot the data
    event_pivot = (
        df.collect()  # Collect before pivot
        .pivot(
            values="year",
            index="PNR",
            on="event_type",
            aggregate_function="len",
        )
        .fill_null(0)
    )

    # Select only numeric columns
    numeric_cols = cs.expand_selector(event_pivot, cs.numeric())
    event_pivot_numeric = event_pivot.select(numeric_cols)

    # Calculate correlation
    corr = event_pivot_numeric.corr()

    fig, ax = plt.subplots(figsize=(14, 12))
    sns.heatmap(corr.to_pandas(), annot=True, cmap="viridis", vmin=-1, vmax=1, center=0, ax=ax)
    ax.set_title("Heatmap of Event Co-occurrences", fontsize=16)
    plt.tight_layout()
    return fig


def plot_stacked_bar(df: pl.LazyFrame, group_col: str) -> Figure:
    """
    Create a stacked bar chart of event distributions across groups.

    This function generates a stacked bar chart showing the distribution of event types
    across different groups specified by the group_col parameter.

    Args:
        df (pl.LazyFrame):
        A LazyFrame containing event data with 'event_type' and the specified group column.
        group_col (str): The name of the column to use for grouping.

    Returns:
        Figure: A matplotlib Figure object containing the stacked bar chart.
    """
    grouped = (
        df.group_by([group_col, "event_type"])
        .count()
        .collect()  # Collect before pivot
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

    fig, ax = plt.subplots(figsize=(14, 8))
    grouped_pct.to_pandas().plot(kind="bar", stacked=True, ax=ax)
    ax.set_title(f"Distribution of Events Across {group_col}", fontsize=16)
    ax.set_xlabel(group_col, fontsize=12)
    ax.set_ylabel("Proportion of Group", fontsize=12)
    ax.legend(bbox_to_anchor=(1.05, 1), loc="upper left", fontsize=10)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    return fig


def plot_sankey(df: pl.LazyFrame, event_sequence: list[str]) -> go.Figure:
    """
    Create a Sankey diagram for a sequence of events.

    This function generates a Sankey diagram showing the flow of events in the specified sequence.

    Args:
        df (pl.LazyFrame):
        A LazyFrame containing event data with 'PNR', 'event_type', and 'year' columns.
        event_sequence (list[str]): A list of event types in the desired sequence.

    Returns:
        go.Figure: A plotly Figure object containing the Sankey diagram.
    """
    # Process the data to get the event flows
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

    # Create source, target, and value lists for Sankey diagram
    source = []
    target = []
    value = []

    for sequence, count in zip(flows["event_sequence"], flows["count"], strict=False):
        events = sequence.split("-")
        for i in range(len(events) - 1):
            source.append(event_sequence.index(events[i]))
            target.append(event_sequence.index(events[i + 1]))
            value.append(count)

    # Create the Sankey diagram
    fig = go.Figure(
        data=[
            go.Sankey(
                node=dict(
                    pad=15,
                    thickness=20,
                    line=dict(color="black", width=0.5),
                    label=event_sequence,
                    color="blue",
                ),
                link=dict(source=source, target=target, value=value),
            )
        ]
    )

    fig.update_layout(title_text="Event Sequence Flow", font_size=10)
    return fig


def plot_survival_curve(df: pl.LazyFrame, event_type: str) -> Figure | None:
    """
    Create a survival curve for a specific event.

    This function generates a Kaplan-Meier survival curve for the specified event type.

    Args:
        df (pl.LazyFrame):
        A LazyFrame containing event data with 'PNR', 'event_type', and 'year' columns.
        event_type (str): The specific event type to analyze.

    Returns:
        Figure | None:
        A matplotlib Figure object containing the survival curve, or None if an error occurs.
    """
    try:
        logger.info(f"Starting survival curve plot for {event_type}")
        event_df = df.filter(pl.col("event_type") == event_type).collect()
        logger.info(f"Filtered data for {event_type}: {event_df.shape} rows")

        min_year = df.select(pl.min("year")).collect().item()
        logger.info(f"Minimum year: {min_year}")
        T = event_df.group_by("PNR").agg(
            time_to_event=(pl.col("year").min() - min_year).cast(pl.Int32)
        )
        logger.info(f"Time to event calculated: {T.shape} rows")

        null_count = T["time_to_event"].null_count()
        if null_count > 0:
            logger.warning(f"{null_count} NaN values found in time calculation for {event_type}")
            T = T.drop_nulls()
            logger.info(f"After dropping nulls: {T.shape} rows")

        E = event_df.group_by("PNR").agg(pl.count("PNR").alias("E"))
        logger.info(f"Event counts calculated: {E.shape} rows")
        T = T.join(E, on="PNR", how="inner")
        logger.info(f"After joining: {T.shape} rows")

        if T.height == 0:
            logger.error(f"No valid data for survival analysis of {event_type}")
            return None

        kmf = KaplanMeierFitter()
        kmf.fit(T["time_to_event"], T["E"], label=event_type)
        logger.info("KaplanMeierFitter fitted successfully")

        fig, ax = plt.subplots(figsize=(12, 8))
        kmf.plot(ax=ax)
        ax.set_title(f"Survival Curve for {event_type}", fontsize=16)
        ax.set_xlabel("Years", fontsize=12)
        ax.set_ylabel("Probability of not experiencing event", fontsize=12)
        ax.grid(True, linestyle="--", alpha=0.7)
        plt.tight_layout()
        logger.info("Survival curve plot created successfully")
        return fig
    except Exception as e:
        logger.error(f"Error in plot_survival_curve for {event_type}: {str(e)}", exc_info=True)
        return None
