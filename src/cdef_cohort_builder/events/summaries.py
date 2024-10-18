import plotly.express as px
import plotly.graph_objects as go
import polars as pl


def generate_summary_table(df: pl.LazyFrame) -> pl.DataFrame:
    """Generate an enhanced summary table of event occurrences."""
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
    """Generate detailed descriptive statistics for numerical variables."""
    # Select only the numeric columns and collect
    numeric_df = df.select(numeric_cols).collect()

    # Generate descriptive statistics
    stats = numeric_df.describe()

    # Transpose the result for better readability
    return stats.transpose(include_header=True, header_name="statistic")


def create_interactive_dashboard(df: pl.LazyFrame) -> go.Figure:
    """Create an enhanced interactive dashboard with multiple visualizations."""
    # Collect necessary data for the dashboard
    dashboard_data = df.select(
        [
            "event_year",
            "event_type",
            "PNR",
            pl.count("PNR").over(["event_year", "event_type"]).alias("count"),
        ]
    ).collect()

    fig = px.scatter(
        dashboard_data.to_pandas(),
        x="event_year",
        y="event_type",
        color="event_type",
        size="count",
        hover_data=["PNR"],
        title="Interactive Event Dashboard",
        labels={"event_year": "Year", "event_type": "Event Type", "count": "Event Count"},
    )

    fig.update_layout(
        xaxis_title="Year",
        yaxis_title="Event Type",
        legend_title="Event Type",
        font=dict(size=12),
    )

    return fig


def generate_event_frequency_analysis(df: pl.LazyFrame) -> dict[str, pl.DataFrame]:
    """Analyze event frequencies over time and by demographic factors."""
    yearly_freq = (
        df.group_by(["event_year", "event_type"])
        .agg(pl.count("PNR").alias("event_count"))
        .sort(["event_year", "event_count"], descending=[False, True])
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
