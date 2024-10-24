import polars as pl
from cdef_cohort.logging_config import logger
from cdef_cohort.mapping_utils import apply_mapping
from cdef_cohort.registers.generic import process_register_data
from cdef_cohort.utils.config import (
    AKM_FILES,
    AKM_OUT,
    POPULATION_FILE,
)
from cdef_cohort.utils.logging_decorator import log_processing
from cdef_cohort.utils.types import KwargsType

AKM_SCHEMA = {
    "PNR": pl.Utf8,
    "SOCIO": pl.Int8,
    "SOCIO02": pl.Int8,
    "SOCIO13": pl.Categorical,  # <- Only one we are interested in
    "CPRTJEK": pl.Utf8,
    "CPRTYPE": pl.Utf8,
    "VERSION": pl.Utf8,
    "SENR": pl.Utf8,  # Dont know the structure of this
}

AKM_DEFAULTS = {
    "population_file": POPULATION_FILE,
    "columns_to_keep": ["PNR", "SOCIO13", "SENR", "year"],
    "join_parents_only": True,
    "longitudinal": False,
}

logger.debug(f"AKM_SCHEMA: {AKM_SCHEMA}")
logger.debug(f"AKM_DEFAULTS: {AKM_DEFAULTS}")


def preprocess_akm(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns(
        pl.col("SOCIO13").cast(pl.Utf8).pipe(apply_mapping, "socio13").cast(pl.Categorical)
    )


@log_processing
def process_akm(**kwargs: KwargsType) -> None:
    """Process AKM data, join with population data, and save the result."""
    process_register_data(
        input_files=AKM_FILES,
        output_file=AKM_OUT,
        schema=AKM_SCHEMA,
        defaults=AKM_DEFAULTS,
        register_name="AKM",
        preprocess=preprocess_akm,
        **kwargs,
    )


if __name__ == "__main__":
    logger.debug("Running process_akm as main")
    process_akm()
    logger.debug("Finished running process_akm as main")
