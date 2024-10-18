from pathlib import Path

import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import seaborn as sns
from lifelines import KaplanMeierFitter


def generate_summary_table(df: pl.DataFrame) -> pl.DataFrame:
    """Generate a summary table of event occurrences."""
    event_counts = df.group_by("event_type").count()
    total_cohort = df["PNR"].n_unique()
    summary = event_counts.with_columns((pl.col("count") / total_cohort * 100).alias("% of Cohort"))
    return summary


def plot_time_series(df: pl.DataFrame):
    """Create a time series plot of event occurrences."""
    event_counts = (
        df.group_by(["year", "event_type"])
        .count()
        .pivot(
            values="count",
            index="year",
            aggregate_function="first",
            on="event_type",
        )
        .fill_null(0)
    )

    plt.figure(figsize=(12, 6))
    for column in event_counts.columns[1:]:  # Skip the 'year' column
        plt.plot(event_counts["year"], event_counts[column], label=column)
    plt.title("Event Occurrences Over Time")
    plt.xlabel("Year")
    plt.ylabel("Number of Occurrences")
    plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()
    return plt.gcf()


def plot_event_heatmap(df: pl.DataFrame):
    """Create a heatmap of event co-occurrences."""
    event_pivot = df.pivot(
        values="year",
        index="PNR",
        aggregate_function="len",
        on="event_type",
    ).fill_null(0)

    corr = event_pivot.corr()
    plt.figure(figsize=(12, 10))
    sns.heatmap(corr.to_pandas(), annot=True, cmap="coolwarm", vmin=-1, vmax=1, center=0)
    plt.title("Heatmap of Event Co-occurrences")
    return plt.gcf()


def plot_stacked_bar(df: pl.DataFrame, group_col: str):
    """Create a stacked bar chart of event distributions across groups."""
    grouped = (
        df.group_by([group_col, "event_type"])
        .count()
        .pivot(
            values="count",
            index=group_col,
            aggregate_function="first",
            on="event_type",
        )
        .fill_null(0)
    )

    grouped_pct = grouped.select(pl.all().exclude(group_col) / pl.all().exclude(group_col).sum())

    fig, _ = plt.subplots(figsize=(12, 6))
    grouped_pct.to_pandas().plot(kind="bar", stacked=True, ax=plt.gca())
    plt.title(f"Distribution of Events Across {group_col}")
    plt.xlabel(group_col)
    plt.ylabel("Proportion of Group")
    plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()
    return fig


def plot_sankey(df: pl.DataFrame, event_sequence):
    """Create a Sankey diagram for a sequence of events."""
    # This function might need more complex logic to handle the new data structure
    # For now, we'll leave it as a placeholder
    fig = go.Figure()
    fig.update_layout(title_text="Event Sequence Flow (Placeholder)", font_size=10)
    return fig


def plot_survival_curve(df: pl.DataFrame, event_type: str):
    """Create a survival curve for a specific event."""
    event_df = df.filter(pl.col("event_type") == event_type)

    # Calculate T (time to event)
    min_year = df["year"].min()
    T = event_df.group_by("PNR").agg(time_to_event=pl.col("year").min() - min_year)

    # Check for NaN values in T
    null_count = T["time_to_event"].null_count()
    if null_count > 0:
        print(f"NaN values found in time calculation for event type: {event_type}")
        print(f"Number of NaN values: {null_count}")
        print("Removing NaN values for survival analysis")
        T = T.drop_nulls()

    # Calculate E (event occurrence)
    E = event_df.group_by("PNR").count().with_columns(pl.lit(1).alias("E"))

    # Ensure T and E have the same index
    T = T.join(E, on="PNR", how="inner")

    # Check if we have any data left after removing NaNs
    if T.height == 0:
        print(f"No valid data left for survival analysis of event type: {event_type}")
        return None

    kmf = KaplanMeierFitter()
    kmf.fit(T["time_to_event"], T["E"], label=event_type)

    fig, ax = plt.subplots(figsize=(10, 6))
    kmf.plot(ax=ax)
    plt.title(f"Survival Curve for {event_type}")
    plt.xlabel("Years")
    plt.ylabel("Probability of not experiencing event")
    return fig


def generate_descriptive_stats(df: pl.DataFrame) -> pl.DataFrame:
    """Generate descriptive statistics for numerical variables."""
    return df.describe()


def create_interactive_dashboard(df: pl.DataFrame):
    """Create an interactive dashboard combining multiple visualizations."""
    fig = px.scatter(df.to_pandas(), x="year", y="event_type", color="event_type")
    fig.update_layout(title="Interactive Event Dashboard")
    return fig


def main(df: pl.DataFrame, output_dir: Path):
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate and save all visualizations
    summary_table = generate_summary_table(df)
    summary_table.write_csv(output_dir / "summary_table.csv")

    time_series_plot = plot_time_series(df)
    time_series_plot.savefig(output_dir / "time_series_plot.png")
    plt.close(time_series_plot)

    heatmap = plot_event_heatmap(df)
    heatmap.savefig(output_dir / "event_heatmap.png")
    plt.close(heatmap)

    sankey = plot_sankey(df, ["event1", "event2", "event3"])  # Replace with actual event names
    sankey.write_html(output_dir / "sankey_diagram.html")

    # Plot survival curve for each event type
    for event_type in df["event_type"].unique():
        survival_curve = plot_survival_curve(df, event_type)
        if survival_curve is not None:
            survival_curve.savefig(output_dir / f"survival_curve_{event_type}.png")
            plt.close(survival_curve)
        else:
            print(f"Skipping survival curve for {event_type} due to insufficient data")

    desc_stats = generate_descriptive_stats(df)
    desc_stats.write_csv(output_dir / "descriptive_stats.csv")

    dashboard = create_interactive_dashboard(df)
    dashboard.write_html(output_dir / "interactive_dashboard.html")

    print(f"All visualizations and tables have been generated and saved to {output_dir}")


if __name__ == "__main__":
    # For testing purposes, you can create a sample DataFrame here
    sample_df = pl.DataFrame(
        {
            "PNR": ["1", "1", "2", "2", "3"],
            "event_type": ["A", "B", "A", "C", "B"],
            "year": [2020, 2021, 2020, 2022, 2021],
        }
    )
    output_directory = Path("event_summaries_output")
    main(sample_df, output_directory)
