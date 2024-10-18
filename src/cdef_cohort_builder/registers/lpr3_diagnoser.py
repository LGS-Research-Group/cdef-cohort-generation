import polars as pl

from cdef_cohort_builder.logging_config import logger
from cdef_cohort_builder.registers.generic import process_register_data
from cdef_cohort_builder.utils.config import (
    LPR3_DIAGNOSER_FILES,
    LPR3_DIAGNOSER_OUT,
)
from cdef_cohort_builder.utils.types import KwargsType

LPR3_DIAGNOSER_SCHEMA = {
    "DW_EK_KONTAKT": pl.Utf8,
    "diagnosekode": pl.Utf8,
    "diagnosetype": pl.Utf8,
    "senere_afkraeftet": pl.Utf8,
    "diagnosekode_parent": pl.Utf8,
    "diagnosetype_parent": pl.Utf8,
    "lprindberetningssystem": pl.Utf8,
}


LPR3_DIAGNOSER_DEFAULTS = {
    "population_file": None,
    "columns_to_keep": ["DW_EK_KONTAKT", "diagnosekode"],
}

logger.debug(f"LPR3_DIAGNOSER_SCHEMA: {LPR3_DIAGNOSER_SCHEMA}")
logger.debug(f"LPR3_DIAGNOSER_DEFAULTS: {LPR3_DIAGNOSER_DEFAULTS}")


def process_lpr3_diagnoser(**kwargs: KwargsType) -> None:
    process_register_data(
        input_files=LPR3_DIAGNOSER_FILES,
        output_file=LPR3_DIAGNOSER_OUT,
        schema=LPR3_DIAGNOSER_SCHEMA,
        defaults=LPR3_DIAGNOSER_DEFAULTS,
        register_name="LPR3_DIAGNOSER",
        **kwargs,
    )


if __name__ == "__main__":
    process_lpr3_diagnoser()
