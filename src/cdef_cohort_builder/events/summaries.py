import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import polars.selectors as cs


def generate_summary_table(df: pl.LazyFrame) -> pl.DataFrame:
    """
    Generate an enhanced summary table of event occurrences.

    This function calculates event counts, percentage of cohort, and percentage of total events
    for each event type in the given DataFrame, including categorical columns.

    Args:
        df (pl.LazyFrame):
            A LazyFrame containing event data with columns 'event_type',
            'PNR', and categorical columns.

    Returns:
        pl.DataFrame:
            A DataFrame with summary statistics for event types and categorical columns.
    """
    event_counts = df.group_by("event_type").agg(pl.count("PNR").alias("count"))
    total_cohort = df.select(pl.n_unique("PNR")).collect().item()

    summary = event_counts.with_columns(
        [
            (pl.col("count") / total_cohort * 100).alias("% of Cohort"),
            (pl.col("count") / pl.sum("count") * 100).alias("% of Total Events"),
        ]
    ).sort("count", descending=True)

    # Add summary for categorical columns
    categorical_cols = df.select(cs.categorical()).columns
    for cat_col in categorical_cols:
        cat_summary = df.group_by(cat_col).agg(pl.count("PNR").alias("count"))
        cat_summary = cat_summary.with_columns(
            [
                (pl.col("count") / total_cohort * 100).alias("% of Cohort"),
                (pl.col("count") / pl.sum("count") * 100).alias("% of Total"),
                pl.lit(cat_col).alias("category"),
            ]
        ).sort("count", descending=True)
        summary = pl.concat([summary, cat_summary])

    return summary.collect()


def generate_descriptive_stats(df: pl.LazyFrame, numeric_cols: list[str]) -> pl.DataFrame:
    """
    Generate detailed descriptive statistics for numerical and categorical variables.

    This function calculates various statistical measures for the specified numeric columns
    and categorical columns.

    Args:
        df (pl.LazyFrame): A LazyFrame containing the data.
        numeric_cols (list[str]): A list of column names for which to calculate statistics.

    Returns:
        pl.DataFrame: A transposed DataFrame with descriptive statistics for each column.
    """
    # Select only the numeric columns and collect
    numeric_df = df.select(numeric_cols).collect()

    # Generate descriptive statistics for numeric columns
    stats = numeric_df.describe()

    # Generate statistics for categorical columns
    categorical_cols = df.select(cs.categorical()).columns
    if categorical_cols:
        cat_stats = df.select(categorical_cols).collect().describe()
        stats = pl.concat([stats, cat_stats], how="vertical")

    # Transpose the result for better readability
    return stats.transpose(include_header=True, header_name="statistic")


def create_interactive_dashboard(df: pl.LazyFrame) -> go.Figure:
    """
    Create an enhanced interactive dashboard with multiple visualizations.

    This function generates a scatter plot dashboard showing event counts over years,
    with interactive features like hover data and color-coding by event type.

    Args:
        df (pl.LazyFrame): A LazyFrame containing event data with columns
        'year', 'event_type', 'PNR', and categorical columns.

    Returns:
        go.Figure: A Plotly Figure object representing the interactive dashboard.
    """
    # Collect necessary data for the dashboard
    categorical_cols = df.select(cs.categorical()).columns
    dashboard_data = df.select(
        [
            "year",
            "event_type",
            "PNR",
            pl.count("PNR").over(["year", "event_type"]).alias("count"),
            *categorical_cols,
        ]
    ).collect()

    fig = px.scatter(
        dashboard_data.to_pandas(),
        x="year",
        y="event_type",
        color="event_type",
        size="count",
        hover_data=["PNR"] + categorical_cols,
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


def generate_event_frequency_analysis(
    df: pl.LazyFrame,
) -> dict[str, pl.DataFrame | dict[str, pl.DataFrame]]:
    """
    Analyze event frequencies over time, by demographic factors, and categorical variables.

    This function generates frequency analyses by year, age group, and categorical columns.

    Args:
        df (pl.LazyFrame): A LazyFrame containing event data with columns
        'year', 'event_type', 'PNR', 'age', and categorical columns.

    Returns:
        dict[str, pl.DataFrame]: A dictionary containing DataFrames with frequency analyses.
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

    # Add frequency analysis for categorical columns
    categorical_cols = df.select(cs.categorical()).columns
    cat_freq = {}
    for cat_col in categorical_cols:
        cat_freq[cat_col] = (
            df.group_by([cat_col, "event_type"])
            .agg(pl.count("PNR").alias("event_count"))
            .sort([cat_col, "event_count"], descending=[False, True])
            .collect()
        )

    return {
        "yearly_frequency": yearly_freq,
        "age_group_frequency": age_group_freq,
        "categorical_frequency": cat_freq,
    }
