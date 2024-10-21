import plotly.express as px
import plotly.graph_objects as go
import polars as pl


def generate_summary_table(df: pl.LazyFrame) -> pl.DataFrame:
    """
    Generate an enhanced summary table of event occurrences.

    This function calculates event counts, percentage of cohort, and percentage of total events
    for each event type in the given DataFrame.

    Args:
        df (pl.LazyFrame): A LazyFrame containing event data with columns 'event_type' and 'PNR'.

    Returns:
        pl.DataFrame:
            A DataFrame with columns 'event_type', 'count', '% of Cohort', and '% of Total Events',
                      sorted by count in descending order.
    """
    event_counts = df.group_by("event_type").agg(pl.count("PNR").alias("count"))
    total_cohort = df.select(pl.n_unique("PNR")).collect().item()

    summary = event_counts.with_columns(
        [
            (pl.col("count") / total_cohort * 100).alias("% of Cohort"),
            (pl.col("count") / pl.sum("count") * 100).alias("% of Total Events"),
        ]
    ).sort("count", descending=True)

    return summary.collect()


def generate_descriptive_stats(df: pl.LazyFrame, numeric_cols: list[str]) -> pl.DataFrame:
    """
    Generate detailed descriptive statistics for numerical variables.

    This function calculates various statistical measures for the specified numeric columns.

    Args:
        df (pl.LazyFrame): A LazyFrame containing the data.
        numeric_cols (list[str]): A list of column names for which to calculate statistics.

    Returns:
        pl.DataFrame: A transposed DataFrame with descriptive statistics for each numeric column.
    """
    # Select only the numeric columns and collect
    numeric_df = df.select(numeric_cols).collect()

    # Generate descriptive statistics
    stats = numeric_df.describe()

    # Transpose the result for better readability
    return stats.transpose(include_header=True, header_name="statistic")


def create_interactive_dashboard(df: pl.LazyFrame) -> go.Figure:
    """
    Create an enhanced interactive dashboard with multiple visualizations.

    This function generates a scatter plot dashboard showing event counts over years,
    with interactive features like hover data and color-coding by event type.

    Args:
        df (pl.LazyFrame): A LazyFrame containing event data with columns
        'year', 'event_type', and 'PNR'.

    Returns:
        go.Figure: A Plotly Figure object representing the interactive dashboard.
    """
    # Collect necessary data for the dashboard
    dashboard_data = df.select(
        [
            "year",
            "event_type",
            "PNR",
            pl.count("PNR").over(["year", "event_type"]).alias("count"),
        ]
    ).collect()

    fig = px.scatter(
        dashboard_data.to_pandas(),
        x="year",
        y="event_type",
        color="event_type",
        size="count",
        hover_data=["PNR"],
        title="Interactive Event Dashboard",
        labels={"year": "Year", "event_type": "Event Type", "count": "Event Count"},
    )

    fig.update_layout(
        xaxis_title="Year",
        yaxis_title="Event Type",
        legend_title="Event Type",
        font=dict(size=12),
    )

    return fig


def generate_event_frequency_analysis(df: pl.LazyFrame) -> dict[str, pl.DataFrame]:
    """
    Analyze event frequencies over time and by demographic factors.

    This function generates two frequency analyses: one by year and another by age group.

    Args:
        df (pl.LazyFrame): A LazyFrame containing event data with columns
        'year', 'event_type', 'PNR', and 'age'.

    Returns:
        dict[str, pl.DataFrame]: A dictionary containing two DataFrames:
            - 'yearly_frequency': Event frequencies by year and event type.
            - 'age_group_frequency': Event frequencies by age group and event type.
    """
    yearly_freq = (
        df.group_by(["year", "event_type"])
        .agg(pl.count("PNR").alias("event_count"))
        .sort(["year", "event_count"], descending=[False, True])
        .collect()
    )

    age_group_freq = (
        df.with_columns(
            pl.when(pl.col("age") <= 18)
            .then(pl.lit("0-18"))
            .when(pl.col("age") <= 30)
            .then(pl.lit("19-30"))
            .when(pl.col("age") <= 50)
            .then(pl.lit("31-50"))
            .when(pl.col("age") <= 70)
            .then(pl.lit("51-70"))
            .otherwise(pl.lit("70+"))
            .alias("age_group")
        )
        .group_by(["age_group", "event_type"])
        .agg(pl.count("PNR").alias("event_count"))
        .sort(["age_group", "event_count"], descending=[False, True])
        .collect()
    )

    return {"yearly_frequency": yearly_freq, "age_group_frequency": age_group_freq}
