import polars as pl

from cdef_cohort_generation.config import BEF_FILES, POPULATION_FILE
from cdef_cohort_generation.utils import parse_dates


def main() -> None:
    # Read all bef parquet files
    bef_files = BEF_FILES
    bef = pl.scan_parquet(bef_files, allow_missing_columns=True).with_columns(
        [parse_dates("FOED_DAG").alias("FOED_DAG_PARSED")]
    )

    # Process children
    children = bef.filter(
        (pl.col("FOED_DAG_PARSED").dt.year() >= 1995)
        & (pl.col("FOED_DAG_PARSED").dt.year() <= 2020)
    ).select(["PNR", "FOED_DAG_PARSED", "FAR_ID", "MOR_ID", "FAMILIE_ID"])

    # Get unique children
    unique_children = (
        children.group_by("PNR")
        .agg(
            [
                pl.col("FOED_DAG_PARSED").first(),
                pl.col("FAR_ID").first(),
                pl.col("MOR_ID").first(),
                pl.col("FAMILIE_ID").first(),
            ]
        )
        .collect()
    )

    # Process parents
    parents = (
        bef.select(["PNR", "FOED_DAG_PARSED"])
        .group_by("PNR")
        .agg(
            [
                pl.col("FOED_DAG_PARSED").first(),
            ]
        )
        .collect()
    )

    # Join children with father and mother
    family = unique_children.join(
        parents.rename({"PNR": "FAR_ID", "FOED_DAG_PARSED": "FAR_FDAG"}),
        on="FAR_ID",
        how="left",
    )

    family = family.join(
        parents.rename({"PNR": "MOR_ID", "FOED_DAG_PARSED": "MOR_FDAG"}),
        on="MOR_ID",
        how="left",
    )

    # Select and arrange final columns in desired order
    family = family.select(
        [
            "PNR",
            pl.col("FOED_DAG_PARSED").alias("FOED_DAG"),
            "FAR_ID",
            "FAR_FDAG",
            "MOR_ID",
            "MOR_FDAG",
            "FAMILIE_ID",
        ]
    )

    # Write result into parquet file
    family.write_parquet(POPULATION_FILE)


if __name__ == "__main__":
    from typing import TYPE_CHECKING

    if not TYPE_CHECKING:
        main()
