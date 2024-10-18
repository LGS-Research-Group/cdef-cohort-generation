import polars as pl

from cdef_cohort_builder.logging_config import logger
from cdef_cohort_builder.registers.generic import process_register_data
from cdef_cohort_builder.utils.config import (
    LPR_BES_FILES,
    LPR_BES_OUT,
)
from cdef_cohort_builder.utils.logging_decorator import log_processing
from cdef_cohort_builder.utils.types import KwargsType

LPR_BES_SCHEMA = {
    "D_AMBDTO": pl.Date,  # Dato for ambulantbesøg
    "LEVERANCEDATO": pl.Date,  # DST leverancedato
    "RECNUM": pl.Utf8,  # LPR-identnummer
    "VERSION": pl.Utf8,  # DST Version
}
LPR_BES_DEFAULTS = {
    "population_file": None,
    "columns_to_keep": ["D_AMBDTO", "RECNUM"],
    "date_columns": ["D_AMBDTO", "LEVERANCEDATO"],
}

logger.debug(f"LPR_BES_SCHEMA: {LPR_BES_SCHEMA}")
logger.debug(f"LPR_BES_DEFAULTS: {LPR_BES_DEFAULTS}")


@log_processing
def process_lpr_bes(**kwargs: KwargsType) -> None:
    process_register_data(
        input_files=LPR_BES_FILES,
        output_file=LPR_BES_OUT,
        schema=LPR_BES_SCHEMA,
        defaults=LPR_BES_DEFAULTS,
        register_name="LPR_BES",
        **kwargs,
    )


if __name__ == "__main__":
    process_lpr_bes()
