import polars as pl
import pyreadr  # type: ignore

from cdef_cohort_generation.config import ISCED_FILE, RDAT_FILE
from cdef_cohort_generation.logging_config import log


def read_isced_data() -> pl.LazyFrame:
    """Read and process ISCED data from Rdata file."""
    try:
        if ISCED_FILE.exists():
            log("Reading ISCED data from existing parquet file...")
            return pl.scan_parquet(ISCED_FILE)
        log("Processing ISCED data from Rdata file...")
        result = pyreadr.read_r(RDAT_FILE, use_objects=["uddf"])
        isced_data = pl.from_dict(result)
        isced_data = isced_data.select(
            pl.col("uddf").struct.field("HFAUDD").cast(pl.Utf8),
            pl.col("uddf").struct.field("HFAUDD_isced"),
        )
        isced_final = (
            isced_data.with_columns(
                [
                    pl.col("HFAUDD").str.strip_suffix(".0"),
                    pl.col("HFAUDD_isced").str.slice(0, 1).alias("EDU_LVL"),
                ],
            )
            .unique()
            .select(["HFAUDD", "EDU_LVL"])
        )
        isced_final.write_parquet(ISCED_FILE)
        return isced_final.lazy()
    except Exception as e:
        log(f"Error processing ISCED data: {e}")
        raise
