from pathlib import Path

import polars as pl

from cdef_cohort_builder.functions.population_summary import save_population_summary
from cdef_cohort_builder.logging_config import log_dataframe_info, logger
from cdef_cohort_builder.utils.config import (
    BEF_FILES,
    BIRTH_INCLUSION_END_YEAR,
    BIRTH_INCLUSION_START_YEAR,
    POPULATION_FILE,
)
from cdef_cohort_builder.utils.date import parse_dates


def main() -> None:
    logger.info(f"Starting population processing with BEF files: {BEF_FILES}")
    logger.info(
        f"Birth inclusion years: {BIRTH_INCLUSION_START_YEAR} to {BIRTH_INCLUSION_END_YEAR}"
    )

    # Read all bef parquet files
    bef_files = BEF_FILES
    logger.debug(f"Reading BEF files: {bef_files}")
    bef = pl.scan_parquet(
        bef_files,
        allow_missing_columns=True,
        schema={
            "PNR": pl.Utf8,
            "FAR_ID": pl.Utf8,
            "MOR_ID": pl.Utf8,
            "FAMILIE_ID": pl.Utf8,
            "FOED_DAG": pl.Utf8,
        },
    ).with_columns(
        [
            parse_dates("FOED_DAG"),
        ],
    )

    logger.debug("BEF data loaded")
    log_dataframe_info(bef, "BEF")

    # Process children
    logger.info("Processing children data")
    children = bef.filter(
        (pl.col("FOED_DAG").dt.year() >= BIRTH_INCLUSION_START_YEAR)
        & (pl.col("FOED_DAG").dt.year() <= BIRTH_INCLUSION_END_YEAR),
    ).select(["PNR", "FOED_DAG", "FAR_ID", "MOR_ID", "FAMILIE_ID"])

    logger.debug("Children data filtered")
    log_dataframe_info(children, "Children")

    # Get unique children
    logger.info("Getting unique children")
    unique_children = (
        children.group_by("PNR")
        .agg(
            [
                pl.col("FOED_DAG").first(),
                pl.col("FAR_ID").first(),
                pl.col("MOR_ID").first(),
                pl.col("FAMILIE_ID").first(),
            ],
        )
        .collect()
    )

    logger.debug("Unique children processed")
    log_dataframe_info(unique_children, "Unique Children")

    # Process parents
    logger.info("Processing parents data")
    parents = (
        bef.select(["PNR", "FOED_DAG"])
        .group_by("PNR")
        .agg(
            [
                pl.col("FOED_DAG").first(),
            ],
        )
        .collect()
    )

    logger.debug("Parents data processed")
    log_dataframe_info(parents, "Parents")

    # Join children with father and mother
    logger.info("Joining children with father data")
    family = unique_children.join(
        parents.rename({"PNR": "FAR_ID", "FOED_DAG": "FAR_FDAG"}),
        on="FAR_ID",
        how="left",
    )
    logger.debug("Father join completed")
    log_dataframe_info(family, "Family after Father Join")

    logger.info("Joining children with mother data")
    family = family.join(
        parents.rename({"PNR": "MOR_ID", "FOED_DAG": "MOR_FDAG"}),
        on="MOR_ID",
        how="left",
    )
    logger.debug("Mother join completed")
    log_dataframe_info(family, "Family after Mother Join")

    # Select and arrange final columns in desired order
    logger.info("Selecting and arranging final columns")
    family = family.select(
        [
            "PNR",
            "FOED_DAG",
            "FAR_ID",
            "FAR_FDAG",
            "MOR_ID",
            "MOR_FDAG",
            "FAMILIE_ID",
        ],
    )

    logger.debug("Final family data prepared")
    log_dataframe_info(family, "Final Family")

    # Ensure the directory exists
    output_dir = Path(POPULATION_FILE).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory created/verified: {output_dir}")

    # Write result into parquet file
    logger.info(f"Writing population data to: {POPULATION_FILE}")
    family.write_parquet(POPULATION_FILE)
    logger.info("Population data writing completed")
    save_population_summary(family, output_dir)


if __name__ == "__main__":
    from typing import TYPE_CHECKING

    if not TYPE_CHECKING:
        logger.info("Starting main function in population.py")
        main()
        logger.info("Finished main function in population.py")
