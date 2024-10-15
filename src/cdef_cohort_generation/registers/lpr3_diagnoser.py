import polars as pl

from cdef_cohort_generation.utils import (
    LPR3_DIAGNOSER_FILES,
    LPR3_DIAGNOSER_OUT,
    KwargsType,
    process_register_data,
)

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


def process_lpr3_diagnoser(**kwargs: KwargsType) -> None:
    process_register_data(
        input_files=LPR3_DIAGNOSER_FILES,
        output_file=LPR3_DIAGNOSER_OUT,
        schema=LPR3_DIAGNOSER_SCHEMA,
        defaults=LPR3_DIAGNOSER_DEFAULTS,
        **kwargs,
    )


if __name__ == "__main__":
    process_lpr3_diagnoser()
