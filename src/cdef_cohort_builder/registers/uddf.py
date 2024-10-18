import polars as pl

from cdef_cohort_builder.logging_config import logger
from cdef_cohort_builder.registers.generic import process_register_data
from cdef_cohort_builder.utils.config import (
    POPULATION_FILE,
    UDDF_FILES,
    UDDF_OUT,
)
from cdef_cohort_builder.utils.logging_decorator import log_processing
from cdef_cohort_builder.utils.types import KwargsType

UDDF_SCHEMA = {
    "PNR": pl.Utf8,
    "CPRTJEK": pl.Utf8,
    "CPRTYPE": pl.Utf8,
    "HFAUDD": pl.Utf8,
    "HF_KILDE": pl.Utf8,
    "HF_VFRA": pl.Utf8,
    "HF_VTIL": pl.Utf8,
    "INSTNR": pl.Int8,
    "VERSION": pl.Utf8,
}


UDDF_DEFAULTS = {
    "population_file": POPULATION_FILE,
    "columns_to_keep": ["PNR", "HFAUDD", "HF_KILDE", "HF_VFRA", "INSTNR"],
    "date_columns": ["HF_VFRA", "HF_VTIL"],
    "join_parents_only": True,
    "register_name": "UDDF",
    "longitudinal": True,
}

logger.debug(f"UDDF_SCHEMA: {UDDF_SCHEMA}")
logger.debug(f"UDDF_DEFAULTS: {UDDF_DEFAULTS}")


@log_processing
def process_uddf(**kwargs: KwargsType) -> None:
    process_register_data(
        input_files=UDDF_FILES,
        output_file=UDDF_OUT,
        schema=UDDF_SCHEMA,
        defaults=UDDF_DEFAULTS,
        register_name="UDDF",
        **kwargs,
    )


if __name__ == "__main__":
    process_uddf()
