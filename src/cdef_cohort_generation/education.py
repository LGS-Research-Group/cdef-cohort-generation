from pathlib import Path

import polars as pl
import pyreadr  # type: ignore

from cdef_cohort_generation.logging_config import log
from cdef_cohort_generation.utils import parse_dates

# Constants
FAM_IN = Path("data/01_family")
EDU_OUT = Path("data/02_education")
EDU_FILES = Path("E:/workdata/708245/data/register/uddf/*.parquet")
ISCED_PATH = Path("data/isced.parquet")
RDAT = Path("E:/workdata/708245/data/register/uddf/uddf.rda")


def read_family_data() -> pl.DataFrame:
    """Read family data from parquet file."""
    try:
        log("Reading family data...")
        return pl.read_parquet(FAM_IN / "cohort.parquet")
    except Exception as e:
        log(f"Error reading family data: {e}")
        raise


def read_isced_data() -> pl.DataFrame:
    """Read and process ISCED data from Rdata file."""
    try:
        if ISCED_PATH.exists():
            log("Reading ISCED data from existing parquet file...")
            return pl.read_parquet(ISCED_PATH)
        else:
            log("Processing ISCED data from Rdata file...")
            result = pyreadr.read_r(RDAT, use_objects=["uddf"])
            isced_data = pl.from_dict(result)
            isced_data = isced_data.select(
                pl.col("uddf").struct.field("HFAUDD").cast(pl.Utf8),
                pl.col("uddf").struct.field("HFAUDD_isced"),
            )
            isced_final = (
                isced_data.with_columns(
                    [
                        pl.col("HFAUDD").str.strip_suffix(".0").alias("EDU_TYPE"),
                        pl.col("HFAUDD_isced").str.slice(0, 1).alias("EDU_LVL"),
                    ]
                )
                .unique()
                .select(["EDU_TYPE", "EDU_LVL"])
            )
            isced_final.write_parquet(ISCED_PATH)
            return isced_final
    except Exception as e:
        log(f"Error processing ISCED data: {e}")
        raise


def read_education_data(isced_data: pl.DataFrame) -> pl.DataFrame:
    """Read and process education data from parquet files."""
    try:
        log("Reading and processing education data...")
        edu_data = (
            pl.scan_parquet(EDU_FILES)
            .filter(pl.col("HFAUDD").cast(pl.Int32) >= 10)
            .with_columns(
                [
                    pl.col("HFAUDD").alias("EDU_TYPE"),
                    parse_dates("HF_VFRA").alias("EDU_DATE"),  # Date of attainment of education
                ]
            )
            .select(
                [
                    "PNR",
                    "EDU_TYPE",
                    "EDU_DATE",
                ]
            )
            .collect()
        )
        return edu_data.join(isced_data, on="EDU_TYPE", how="left")
    except Exception as e:
        log(f"Error reading education data: {e}")
        raise


def process_education_data(edu: pl.DataFrame) -> pl.DataFrame:
    """Process education data to get highest education level and type."""
    try:
        log("Processing education data...")
        return edu.group_by("PNR").agg(
            [
                pl.col("EDU_LVL").cast(pl.Int8).max().alias("highest_edu_level"),
                pl.col("EDU_TYPE")
                .filter(pl.col("EDU_LVL") == pl.col("EDU_LVL").max())
                .first()
                .alias("highest_edu_type"),
                pl.col("EDU_DATE").max().alias("latest_education_date"),
            ]
        )
    except Exception as e:
        log(f"Error processing education data: {e}")
        raise


def join_family_and_education(family: pl.DataFrame, edu_processed: pl.DataFrame) -> pl.DataFrame:
    """Join family and education data."""
    try:
        log("Joining family and education data...")
        result = family.join(
            edu_processed.rename(
                {
                    "PNR": "FAR_ID",
                    "highest_edu_level": "FAR_EDU_LVL",
                    "highest_edu_type": "FAR_EDU_TYPE",
                    "latest_education_date": "FAR_EDU_DATE",
                }
            ),
            on="FAR_ID",
            how="left",
        ).join(
            edu_processed.rename(
                {
                    "PNR": "MOR_ID",
                    "highest_edu_level": "MOR_EDU_LVL",
                    "highest_edu_type": "MOR_EDU_TYPE",
                    "latest_education_date": "MOR_EDU_DATE",
                }
            ),
            on="MOR_ID",
            how="left",
        )

        # Add derived columns
        log("Adding derived columns...")
        result = result.with_columns(
            [
                # Identify if parents have tertiary education
                (pl.col("FAR_EDU_LVL") >= 6).alias("FAR_HAS_TERTIARY"),
                (pl.col("MOR_EDU_LVL") >= 6).alias("MOR_HAS_TERTIARY"),
                # Check if parents have different education levels
                (pl.col("FAR_EDU_LVL") != pl.col("MOR_EDU_LVL")).alias("PARENTS_DIFF_EDUCATION"),
                # Calculate years since education for father
                pl.when(pl.col("FAR_EDU_DATE").is_not_null())
                .then(pl.col("FOED_DAG").dt.year() - pl.col("FAR_EDU_DATE").dt.year())
                .otherwise(None)
                .alias("FAR_YEARS_SINCE_EDUCATION"),
                # Calculate years since education for mother
                pl.when(pl.col("MOR_EDU_DATE").is_not_null())
                .then(pl.col("FOED_DAG").dt.year() - pl.col("MOR_EDU_DATE").dt.year())
                .otherwise(None)
                .alias("MOR_YEARS_SINCE_EDUCATION"),
            ]
        )

        return result
    except Exception as e:
        log(f"Error joining family and education data: {e}")
        raise


def main() -> None:
    try:
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
        log("Selecting final columns...")
        final_columns = [
            "PNR",
            "FOED_DAG",
            "FAR_ID",
            "FAR_FDAG",
            "MOR_ID",
            "MOR_FDAG",
            "FAMILIE_ID",
            "FAR_EDU_LVL",
            "FAR_EDU_TYPE",
            "FAR_EDU_DATE",
            "MOR_EDU_LVL",
            "MOR_EDU_TYPE",
            "MOR_EDU_DATE",
            "FAR_HAS_TERTIARY",
            "MOR_HAS_TERTIARY",
            "PARENTS_DIFF_EDUCATION",
            "FAR_YEARS_SINCE_EDUCATION",
            "MOR_YEARS_SINCE_EDUCATION",
        ]
        final_result = result.select(final_columns)

        # Write result to parquet file
        log(f"Writing data to {EDU_OUT / 'cohort.parquet'}...")
        final_result.write_parquet(EDU_OUT / "cohort.parquet")
        log("Data processing completed successfully.")
    except Exception as e:
        log(f"An error occurred in the main function: {e}")
        raise


if __name__ == "__main__":
    main()
