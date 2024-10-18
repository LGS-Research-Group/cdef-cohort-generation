import polars as pl

from cdef_cohort_builder.logging_config import logger
from cdef_cohort_builder.registers.generic import process_register_data
from cdef_cohort_builder.utils.config import (
    LPR3_KONTAKTER_FILES,
    LPR3_KONTAKTER_OUT,
)
from cdef_cohort_builder.utils.types import KwargsType

LPR3_KONTAKTER_SCHEMA = {
    "SORENHED_IND": pl.Utf8,
    "SORENHED_HEN": pl.Utf8,
    "SORENHED_ANS": pl.Utf8,
    "DW_EK_KONTAKT": pl.Utf8,
    "DW_EK_FORLOEB": pl.Utf8,
    "CPR": pl.Utf8,
    "dato_start": pl.Utf8,
    "tidspunkt_start": pl.Utf8,
    "dato_slut": pl.Utf8,
    "tidspunkt_slut": pl.Utf8,
    "aktionsdiagnose": pl.Utf8,
    "kontaktaarsag": pl.Utf8,
    "prioritet": pl.Utf8,
    "kontakttype": pl.Utf8,
    "henvisningsaarsag": pl.Utf8,
    "henvisningsmaade": pl.Utf8,
    "dato_behandling_start": pl.Utf8,
    "tidspunkt_behandling_start": pl.Utf8,
    "dato_indberetning": pl.Utf8,
    "lprindberetningssytem": pl.Utf8,
}


LPR3_KONTAKTER_DEFAULTS = {
    "population_file": None,
    "columns_to_keep": ["DW_EK_KONTAKT", "CPR", "dato_start", "aktionsdiagnose", "dato_slut"],
    "date_columns": ["dato_slut", "dato_start", "dato_behandling_start", "dato_indberetning"],
    "register_name": "LPR3_KONTAKTER",
}

logger.debug(f"LPR3_KONTAKTER_SCHEMA: {LPR3_KONTAKTER_SCHEMA}")
logger.debug(f"LPR3_KONTAKER_DEFAULTS: {LPR3_KONTAKTER_DEFAULTS}")


def process_lpr3_kontakter(**kwargs: KwargsType) -> None:
    process_register_data(
        input_files=LPR3_KONTAKTER_FILES,
        output_file=LPR3_KONTAKTER_OUT,
        schema=LPR3_KONTAKTER_SCHEMA,
        defaults=LPR3_KONTAKTER_DEFAULTS,
        **kwargs,
    )


if __name__ == "__main__":
    process_lpr3_kontakter()
