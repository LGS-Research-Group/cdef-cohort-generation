from pathlib import Path

import polars as pl
import pyreadr  # type: ignore

# Constants
FAM_IN = Path("data/01_family")
EDU_OUT = Path("data/02_education")
EDU_FILES = Path("E:/workdata/708245/data/register/uddf/*.parquet")
ISCED_PATH = Path("data/isced.parquet")
RDAT = Path("E:/workdata/708245/data/register/uddf/uddf.rda")


def read_family_data() -> pl.DataFrame:
    """Read family data from parquet file."""
    return pl.read_parquet(FAM_IN / "cohort.parquet")


def read_isced_data() -> pl.DataFrame:
    """Read and process ISCED data from Rdata file."""
    if ISCED_PATH.exists():
        return pl.read_parquet(ISCED_PATH)
    else:
        # Read the Rdata file
        result = pyreadr.read_r(RDAT, use_objects=["uddf"])

        isced_data = pl.from_dict(result)

        isced_data = isced_data.select(
            pl.col("uddf").struct.field("HFAUDD").cast(pl.Utf8),
            pl.col("uddf").struct.field("HFAUDD_isced"),
        )

        isced_final = (
            isced_data.with_columns(
                [
                    pl.col("HFAUDD").str.strip_suffix(".0"),
                    pl.col("HFAUDD_isced").str.slice(0, 1).alias("ISCED_LEVEL"),
                ]
            )
            .unique()
            .select(["HFAUDD", "ISCED_LEVEL"])
        )

        isced_final.write_parquet(ISCED_PATH)

        return isced_final


def read_education_data(isced_data: pl.DataFrame) -> pl.DataFrame:
    """Read and process education data from parquet files."""
    edu_data = (
        pl.scan_parquet(EDU_FILES)
        .filter(pl.col("AUDD") >= 10)
        .with_columns(
            [
                pl.col("AUDD").cast(pl.Utf8).alias("HFAUDD"),
                pl.col("AUDD").cast(pl.Utf8).str.slice(0, 1).alias("education_level"),
                pl.col("AUDD").cast(pl.Utf8).str.slice(1, 2).alias("education_type"),
                pl.col("ALDER").cast(pl.Int32),
                pl.col("HF_VFRA").cast(pl.Date),
                pl.col("HF_VTIL").cast(pl.Date),
            ]
        )
        .select(
            [
                "PNR",
                "HFAUDD",
                "education_level",
                "education_type",
                "ALDER",
                "HF_VFRA",
                "HF_VTIL",
            ]
        )
        .collect()
    )
    return edu_data.join(isced_data, on="HFAUDD", how="left")


def process_education_data(edu: pl.DataFrame) -> pl.DataFrame:
    """Process education data to get highest education level and type."""
    return edu.group_by("PNR").agg(
        [
            pl.col("ISCED_LEVEL").cast(pl.Int8).max().alias("highest_isced_level"),
            pl.col("education_level").max().alias("highest_education_level"),
            pl.col("education_type")
            .filter(pl.col("education_level") == pl.col("education_level").max())
            .first()
            .alias("highest_education_type"),
            pl.col("ALDER").max().alias("age_at_highest_education"),
            pl.col("HF_VFRA").max().alias("latest_education_date"),
        ]
    )


def join_family_and_education(family: pl.DataFrame, edu_processed: pl.DataFrame) -> pl.DataFrame:
    """Join family and education data."""
    result = family.join(
        edu_processed.rename(
            {
                "PNR": "FAR_ID",
                "highest_isced_level": "FAR_ISCED_LEVEL",
                "highest_education_level": "FAR_EDU_LVL",
                "highest_education_type": "FAR_EDU_TYPE",
                "age_at_highest_education": "FAR_EDU_AGE",
                "latest_education_date": "FAR_EDU_DATE",
            }
        ),
        on="FAR_ID",
        how="left",
    ).join(
        edu_processed.rename(
            {
                "PNR": "MOR_ID",
                "highest_isced_level": "MOR_ISCED_LEVEL",
                "highest_education_level": "MOR_EDU_LVL",
                "highest_education_type": "MOR_EDU_TYPE",
                "age_at_highest_education": "MOR_EDU_AGE",
                "latest_education_date": "MOR_EDU_DATE",
            }
        ),
        on="MOR_ID",
        how="left",
    )

    # Add derived columns
    result = result.with_columns(
        [
            (pl.col("FAR_ISCED_LEVEL") >= 6).alias("FAR_HAS_TERTIARY"),
            (pl.col("MOR_ISCED_LEVEL") >= 6).alias("MOR_HAS_TERTIARY"),
            (pl.col("FAR_EDU_LVL") != pl.col("MOR_EDU_LVL")).alias("PARENTS_DIFF_EDUCATION"),
            pl.when(pl.col("FAR_EDU_DATE").is_not_null())
            .then(pl.col("FOED_DAG").dt.year() - pl.col("FAR_EDU_DATE").dt.year())
            .otherwise(None)
            .alias("FAR_YEARS_SINCE_EDUCATION"),
            pl.when(pl.col("MOR_EDU_DATE").is_not_null())
            .then(pl.col("FOED_DAG").dt.year() - pl.col("MOR_EDU_DATE").dt.year())
            .otherwise(None)
            .alias("MOR_YEARS_SINCE_EDUCATION"),
        ]
    )

    return result


def main() -> None:
    # Ensure output directory exists
    EDU_OUT.mkdir(parents=True, exist_ok=True)

    # Read family data
    family = read_family_data()

    # Read ISCED data
    isced_data = read_isced_data()

    # Read and process education data
    edu = read_education_data(isced_data)
    edu_processed = process_education_data(edu)

    # Join family and education data
    result = join_family_and_education(family, edu_processed)

    # Select final columns
    final_columns = [
        "PNR",
        "FOED_DAG",
        "FAR_ID",
        "FAR_FDAG",
        "MOR_ID",
        "MOR_FDAG",
        "FAMILIE_ID",
        "FAR_ISCED_LEVEL",
        "FAR_EDU_LVL",
        "FAR_EDU_TYPE",
        "FAR_EDU_AGE",
        "FAR_EDU_DATE",
        "MOR_ISCED_LEVEL",
        "MOR_EDU_LVL",
        "MOR_EDU_TYPE",
        "MOR_EDU_AGE",
        "MOR_EDU_DATE",
        "FAR_HAS_TERTIARY",
        "MOR_HAS_TERTIARY",
        "PARENTS_DIFF_EDUCATION",
        "FAR_YEARS_SINCE_EDUCATION",
        "MOR_YEARS_SINCE_EDUCATION",
    ]
    final_result = result.select(final_columns)

    # Write result to parquet file
    final_result.write_parquet(EDU_OUT / "cohort.parquet")
    print(f"Data written to {EDU_OUT / 'cohort.parquet'}")


if __name__ == "__main__":
    from typing import TYPE_CHECKING

    if not TYPE_CHECKING:
        main()
