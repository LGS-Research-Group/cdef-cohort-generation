import polars as pl

from cdef_cohort_builder.logging_config import logger
from cdef_cohort_builder.registers.generic import process_register_data
from cdef_cohort_builder.utils.config import (
    IDAN_FILES,
    IDAN_OUT,
    POPULATION_FILE,
)
from cdef_cohort_builder.utils.logging_decorator import log_processing
from cdef_cohort_builder.utils.types import KwargsType

IDAN_SCHEMA = {
    "ARBGNR": pl.Utf8,  # Arbejdsgivernummer
    "ARBNR": pl.Utf8,  # Arbejdsstedsnummer
    "CPRTJEK": pl.Utf8,
    "CPRTYPE": pl.Utf8,
    "CVRNR": pl.Utf8,
    "JOBKAT": pl.Int8,  # See JOBKAT_map
    "JOBLON": pl.Float64,  # salary
    "LBNR": pl.Utf8,
    "PNR": pl.Utf8,
    "STILL": pl.Utf8,  # a variation of job title
    "TILKNYT": pl.Int8,  # See TILKNYT_map
}


IDAN_DEFAULTS = {
    "population_file": POPULATION_FILE,
    "columns_to_keep": [
        "PNR",
        "ARBGNR",
        "ARBNR",
        "CVRNR",
        "JOBKAT",
        "JOBLON",
        "LBNR",
        "STILL",
        "TILKNYT",
        "year",
    ],
    "join_parents_only": True,
    "longitudinal": True,
}

logger.debug(f"IDAN_SCHEMA: {IDAN_SCHEMA}")
logger.debug(f"IDAN_DEFAULTS: {IDAN_DEFAULTS}")


@log_processing
def process_idan(**kwargs: KwargsType) -> None:
    process_register_data(
        input_files=IDAN_FILES,
        output_file=IDAN_OUT,
        schema=IDAN_SCHEMA,
        defaults=IDAN_DEFAULTS,
        register_name="IDAN",
        **kwargs,
    )


if __name__ == "__main__":
    process_idan()
