from pathlib import Path

import polars as pl

from cdef_cohort.functions.population_summary import save_population_summary
from cdef_cohort.logging_config import logger
from cdef_cohort.utils.config import (
    BEF_FILES,
    BIRTH_INCLUSION_END_YEAR,
    BIRTH_INCLUSION_START_YEAR,
    MFR_FILES,  # Need to add this to config
    POPULATION_FILE,
)
from cdef_cohort.utils.date import parse_dates


def read_bef_data() -> pl.LazyFrame:
    """Read BEF data and return a LazyFrame with standardized columns."""
    logger.info(f"Reading BEF data from: {BEF_FILES}")

    return (
        pl.read_parquet(
            BEF_FILES,
            columns=["PNR", "FAR_ID", "MOR_ID", "FAMILIE_ID", "FOED_DAG"],
            schema={
                "PNR": pl.Utf8,
                "FAR_ID": pl.Utf8,
                "MOR_ID": pl.Utf8,
                "FAMILIE_ID": pl.Utf8,
                "FOED_DAG": pl.Utf8,
            },
        )
        .lazy()
        .with_columns([parse_dates("FOED_DAG")])
    )


def read_mfr_data() -> pl.LazyFrame:
    """Read MFR data and return a LazyFrame with standardized columns."""
    logger.info(f"Reading MFR data from: {MFR_FILES}")

    return (
        pl.read_parquet(
            MFR_FILES,
            columns=["CPR_BARN", "CPR_FADER", "CPR_MODER", "FOEDSELSDATO"],
            schema={
                "CPR_BARN": pl.Utf8,
                "CPR_FADER": pl.Utf8,
                "CPR_MODER": pl.Utf8,
                "FOEDSELSDATO": pl.Utf8,
            },
        )
        .lazy()
        .with_columns([parse_dates("FOEDSELSDATO")])
        .select(
            [
                pl.col("CPR_BARN").alias("PNR"),
                pl.col("CPR_FADER").alias("FAR_ID"),
                pl.col("CPR_MODER").alias("MOR_ID"),
                pl.col("FOEDSELSDATO").alias("FOED_DAG"),
                pl.lit(None).cast(pl.Utf8).alias("FAMILIE_ID"),
            ]
        )
    )


def get_unique_children(df: pl.LazyFrame) -> pl.DataFrame:
    """Filter and get unique children from the data."""
    return (
        df.filter(
            (pl.col("FOED_DAG").dt.year() >= BIRTH_INCLUSION_START_YEAR)
            & (pl.col("FOED_DAG").dt.year() <= BIRTH_INCLUSION_END_YEAR),
        )
        .select(["PNR", "FOED_DAG", "FAR_ID", "MOR_ID", "FAMILIE_ID"])
        .group_by("PNR")
        .agg(
            [
                pl.col("FOED_DAG").first(),
                pl.col("FAR_ID").first(),
                pl.col("MOR_ID").first(),
                pl.col("FAMILIE_ID").first(),
            ]
        )
        .collect()
    )


def create_data_summary(df: pl.DataFrame, prefix: str) -> dict[str, int]:
    """Create summary statistics for a dataset."""
    return {
        f"total_{prefix}_records": len(df),
        f"{prefix}_missing_far": df["FAR_ID"].null_count(),
        f"{prefix}_missing_mor": df["MOR_ID"].null_count(),
    }


def combine_children_data(
    bef_children: pl.DataFrame, mfr_children: pl.DataFrame
) -> tuple[pl.DataFrame, dict[str, int], dict[str, int]]:
    """Combine BEF and MFR children data."""
    # Create summaries before merge
    summary_before = {
        **create_data_summary(bef_children, "bef"),
        **create_data_summary(mfr_children, "mfr"),
    }

    # Combine data
    combined = (
        bef_children.join(mfr_children, on="PNR", how="full", suffix="_mfr")
        .with_columns(
            [
                pl.coalesce("FAR_ID", "FAR_ID_mfr").alias("FAR_ID"),
                pl.coalesce("MOR_ID", "MOR_ID_mfr").alias("MOR_ID"),
                pl.coalesce("FOED_DAG", "FOED_DAG_mfr").alias("FOED_DAG"),
                pl.col("FAMILIE_ID"),
            ]
        )
        .drop(["FAR_ID_mfr", "MOR_ID_mfr", "FOED_DAG_mfr", "FAMILIE_ID_mfr"])
    )

    # Create summary after merge
    summary_after = {
        "total_combined_records": len(combined),
        "combined_missing_far": combined["FAR_ID"].null_count(),
        "combined_missing_mor": combined["MOR_ID"].null_count(),
        "records_only_in_bef": len(combined.filter(pl.col("FAMILIE_ID").is_not_null())),
        "records_only_in_mfr": len(
            combined.filter(pl.col("FAMILIE_ID").is_null() & pl.col("PNR").is_not_null())
        ),
    }

    return combined, summary_before, summary_after


def process_parents(bef_data: pl.LazyFrame) -> pl.DataFrame:
    """Process parent information from BEF data."""
    return (
        bef_data.select(["PNR", "FOED_DAG"])
        .group_by("PNR")
        .agg([pl.col("FOED_DAG").first()])
        .collect()
    )


def create_family_data(children: pl.DataFrame, parents: pl.DataFrame) -> pl.DataFrame:
    """Create final family dataset with parent information."""
    # Join with fathers
    family = children.join(
        parents.rename({"PNR": "FAR_ID", "FOED_DAG": "FAR_FDAG"}),
        on="FAR_ID",
        how="left",
    )

    # Join with mothers
    family = family.join(
        parents.rename({"PNR": "MOR_ID", "FOED_DAG": "MOR_FDAG"}),
        on="MOR_ID",
        how="left",
    )

    # Select final columns
    return family.select(
        [
            "PNR",
            "FOED_DAG",
            "FAR_ID",
            "FAR_FDAG",
            "MOR_ID",
            "MOR_FDAG",
            "FAMILIE_ID",
        ]
    )


def main() -> None:
    logger.info("Starting population processing")

    # Read data
    bef_data = read_bef_data()
    mfr_data = read_mfr_data()

    # Process children
    bef_children = get_unique_children(bef_data)
    mfr_children = get_unique_children(mfr_data)

    # Combine children data
    combined_children, summary_before, summary_after = combine_children_data(
        bef_children, mfr_children
    )

    # Process parents and create final family data
    parents = process_parents(bef_data)
    family = create_family_data(combined_children, parents)

    # Save results
    output_dir = Path(POPULATION_FILE).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save summaries
    pl.DataFrame(summary_before).write_parquet(output_dir / "population_summary_before.parquet")
    pl.DataFrame(summary_after).write_parquet(output_dir / "population_summary_after.parquet")

    # Save final dataset
    family.write_parquet(POPULATION_FILE)
    save_population_summary(family, output_dir)

    logger.info("Population processing completed")


if __name__ == "__main__":
    logger.info("Starting main function in population.py")
    main()
    logger.info("Finished main function in population.py")
