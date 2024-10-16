import json

import polars as pl

from cdef_cohort_generation.logging_config import log
from cdef_cohort_generation.utils.config import ISCED_FILE, ISCED_MAPPING_FILE


def read_isced_data() -> pl.LazyFrame:
    """Read and process ISCED data from TSV-like file."""
    try:
        if ISCED_FILE.exists():
            log("Reading ISCED data from existing parquet file...")
            return pl.scan_parquet(ISCED_FILE)

        log("Processing ISCED data from TSV-like file...")

        # Read the JSON file
        with open(ISCED_MAPPING_FILE) as json_file:
            isced_data = json.load(json_file)

        # Convert the JSON data to a Polars DataFrame
        isced_df = pl.DataFrame(
            [{"HFAUDD": key, "EDU_LVL": value} for key, value in isced_data.items()]
        )

        # Process the data
        isced_final = (
            isced_df.with_columns(
                [
                    pl.col("HFAUDD").cast(pl.Utf8),
                    pl.col("EDU_LVL").cast(pl.Utf8),
                ]
            )
            .unique()
            .select(["HFAUDD", "EDU_LVL"])
        )

        # Write to parquet file
        isced_final.write_parquet(ISCED_FILE)

        log("ISCED data processed and saved to parquet file.")

        return isced_final.lazy()
    except Exception as e:
        log(f"Error processing ISCED data: {e}")
        raise
