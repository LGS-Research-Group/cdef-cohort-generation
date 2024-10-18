import polars as pl

from cdef_cohort_builder.logging_config import logger
from cdef_cohort_builder.registers.generic import process_register_data
from cdef_cohort_builder.utils.config import (
    IND_FILES,
    IND_OUT,
    POPULATION_FILE,
)
from cdef_cohort_builder.utils.types import KwargsType

IND_SCHEMA = {
    "BESKST13": pl.Int8,  # Kode for personens væsentligste indkomstkilde
    "CPRTJEK": pl.Utf8,
    "CPRTYPE": pl.Utf8,
    "LOENMV_13": pl.Float64,  # Lønindkomst
    "PERINDKIALT_13": pl.Float64,  # Personlig indkomst
    "PNR": pl.Utf8,
    "PRE_SOCIO": pl.Int8,  # See mapping
    "VERSION": pl.Utf8,
}


IND_DEFAULTS = {
    "population_file": POPULATION_FILE,
    "columns_to_keep": ["PNR", "BESKST13", "LOENMV_13", "PERINDKIALT_13", "PRE_SOCIO", "year"],
    "join_parents_only": True,
    "longitudinal": True,
}

logger.debug(f"IND_SCHEMA: {IND_SCHEMA}")
logger.debug(f"IND_DEFAULTS: {IND_DEFAULTS}")


def process_ind(**kwargs: KwargsType) -> None:
    process_register_data(
        input_files=IND_FILES,
        output_file=IND_OUT,
        schema=IND_SCHEMA,
        defaults=IND_DEFAULTS,
        register_name="IND",
        **kwargs,
    )


if __name__ == "__main__":
    process_ind()
