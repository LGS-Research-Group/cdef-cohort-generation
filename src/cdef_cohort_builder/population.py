from pathlib import Path

import polars as pl

from cdef_cohort_builder.logging_config import logger
from cdef_cohort_builder.utils.config import (
    BEF_FILES,
    BIRTH_INCLUSION_END_YEAR,
    BIRTH_INCLUSION_START_YEAR,
    POPULATION_FILE,
)
from cdef_cohort_builder.utils.date import parse_dates


def main() -> None:
    logger.debug(f"Starting population processing with BEF files: {BEF_FILES}")
    logger.debug(
        f"Birth inclusion years: {BIRTH_INCLUSION_START_YEAR} to {BIRTH_INCLUSION_END_YEAR}"
    )

    # Read all bef parquet files
    bef_files = BEF_FILES
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

    logger.debug(f"BEF schema: {bef.collect_schema()}")

    # Process children
    children = bef.filter(
        (pl.col("FOED_DAG").dt.year() >= BIRTH_INCLUSION_START_YEAR)
        & (pl.col("FOED_DAG").dt.year() <= BIRTH_INCLUSION_END_YEAR),
    ).select(["PNR", "FOED_DAG", "FAR_ID", "MOR_ID", "FAMILIE_ID"])

    logger.debug(f"Children schema after filtering: {children.collect_schema()}")

    # Get unique children
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

    logger.debug(f"Unique children count: {unique_children.shape[0]}")

    # Process parents
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

    logger.debug(f"Parents count: {parents.shape[0]}")

    # Join children with father and mother
    family = unique_children.join(
        parents.rename({"PNR": "FAR_ID", "FOED_DAG": "FAR_FDAG"}),
        on="FAR_ID",
        how="left",
    )

    family = family.join(
        parents.rename({"PNR": "MOR_ID", "FOED_DAG": "MOR_FDAG"}),
        on="MOR_ID",
        how="left",
    )

    # Select and arrange final columns in desired order
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

    # Debug: Print schema of family dataframe
    logger.debug(f"Family schema: {family.schema}")
    logger.debug(f"Final family dataframe shape: {family.shape}")

    # Ensure the directory exists
    output_dir = Path(POPULATION_FILE).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Output directory created/verified: {output_dir}")

    # Write result into parquet file
    family.write_parquet(POPULATION_FILE)
    logger.debug(f"Population data written to: {POPULATION_FILE}")


if __name__ == "__main__":
    from typing import TYPE_CHECKING

    if not TYPE_CHECKING:
        logger.debug("Starting main function in population.py")
        main()
        logger.debug("Finished main function in population.py")
