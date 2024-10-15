
import polars as pl

from cdef_cohort_generation.logging_config import log
from cdef_cohort_generation.utils.config import ISCED_FILE, ISCED_TSV_FILE


def read_isced_data() -> pl.LazyFrame:
    """Read and process ISCED data from TSV-like file."""
    try:
        if ISCED_FILE.exists():
            log("Reading ISCED data from existing parquet file...")
            return pl.scan_parquet(ISCED_FILE)

        log("Processing ISCED data from TSV-like file...")

        # Read the TSV-like file
        isced_data = pl.read_csv(
            ISCED_TSV_FILE,
            separator="=",
            has_header=False,
            new_columns=["HFAUDD", "ISCED_LEVEL"],
            skip_rows=0,
            truncate_ragged_lines=True
        )

        # Process the data, explicitly casting to strings
        isced_final = (
            isced_data.with_columns([
                pl.col("HFAUDD").cast(pl.Utf8).str.strip_chars("'"),
                pl.col("ISCED_LEVEL").cast(pl.Utf8).str.strip_chars("'").alias("EDU_LVL"),
            ])
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
