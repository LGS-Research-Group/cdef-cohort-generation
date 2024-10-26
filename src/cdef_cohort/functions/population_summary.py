from pathlib import Path

import polars as pl


def save_population_summary(family_df: pl.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary = {}

    # Table 1: Basic statistics
    summary["basic_stats"] = family_df.describe()
    summary["basic_stats"].write_csv(output_dir / "basic_stats.csv")

    # Table 2: Missing data summary
    missing_counts = family_df.null_count()
    summary["missing_data"] = pl.DataFrame(
        {
            "Column": missing_counts.columns,
            "Missing Values": missing_counts.row(0),
            "Percentage": (100 * pl.Series(missing_counts.row(0)) / len(family_df)).round(2),
        }
    )
    summary["missing_data"].write_csv(output_dir / "missing_data.csv")

    # Data for Figure 1: Age distribution of children
    family_df.select(pl.col("FOED_DAG")).write_csv(output_dir / "child_age_distribution.csv")

    # Data for Figure 2: Age difference between children and parents
    age_diff_data = family_df.select(
        ((pl.col("FOED_DAG") - pl.col("FAR_FDAG")).dt.total_days() / 365.25).alias(
            "father_age_diff"
        ),
        ((pl.col("FOED_DAG") - pl.col("MOR_FDAG")).dt.total_days() / 365.25).alias(
            "mother_age_diff"
        ),
    )
    age_diff_data.write_csv(output_dir / "parent_child_age_diff.csv")


def save_event_summary(events_df: pl.LazyFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    # Table 1: Event counts
    event_counts = events_df.group_by("event_type").count().collect()
    event_counts.write_csv(output_dir / "event_counts.csv")

    # Table 2: Event timing statistics
    event_timing = (
        events_df.group_by("event_type")
        .agg(
            [
                pl.col("event_date").min().alias("min"),
                pl.col("event_date").max().alias("max"),
                pl.col("event_date").mean().alias("mean"),
                pl.col("event_date").median().alias("median"),
            ]
        )
        .collect()
    )
    event_timing.write_csv(output_dir / "event_timing.csv")

    # Data for Figure 1: Event distribution over time
    event_distribution = events_df.select(pl.col("event_date"), pl.col("event_type")).collect()
    event_distribution.write_csv(output_dir / "event_distribution.csv")

    # Data for Figure 2: Event type proportions
    event_proportions = (
        events_df.group_by("event_type").count().sort("count", descending=True).collect()
    )
    event_proportions.write_csv(output_dir / "event_proportions.csv")


def save_scd_summary(scd_data: pl.LazyFrame, output_file: Path) -> None:
    """Save summary of severe chronic disease data."""
    # Collect and flatten the data
    summary = scd_data.collect().with_columns(
        [
            pl.col("is_scd").fill_null(False),
            pl.col("first_scd_date").cast(pl.Utf8),  # Convert date to string
        ]
    )

    # Calculate summary statistics
    total_cases = summary.filter(pl.col("is_scd")).height
    total_population = summary.height
    prevalence = (total_cases / total_population) * 100 if total_population > 0 else 0

    # Create a simple summary DataFrame
    summary_df = pl.DataFrame(
        {
            "metric": ["Total Population", "SCD Cases", "Prevalence (%)"],
            "value": [total_population, total_cases, round(prevalence, 2)],
        }
    )

    # Save to CSV
    summary_df.write_csv(output_file)


def save_all_summaries(
    family_df: pl.DataFrame, events_df: pl.LazyFrame, scd_data: pl.LazyFrame, output_dir: Path
) -> None:
    save_population_summary(family_df, output_dir / "population_summary")
    save_event_summary(events_df, output_dir / "event_summary")
    save_scd_summary(scd_data, output_dir / "scd_summary")
