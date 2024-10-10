import polars as pl

from cdef_cohort_generation.config import LPR3_DIAGNOSER_FILES, LPR3_DIAGNOSER_OUT, POPULATION_FILE
from cdef_cohort_generation.utils import process_register_data

LPR3_DIAGNOSER_SCHEMA = {
    "DW_EK_KONTAKT": pl.Utf8,
    "diagnosekode": pl.Utf8,
    "diagnosetype": pl.Utf8,
    "senere_afkraeftet": pl.Utf8,
    "diagnosekode_parent": pl.Utf8,
    "diagnosetype_parent": pl.Utf8,
    "lprindberetningssystem": pl.Utf8,
}


def process_lpr3_diagnoser() -> None:
    process_register_data(
        input_files=LPR3_DIAGNOSER_FILES,
        output_file=LPR3_DIAGNOSER_OUT,
        population_file=POPULATION_FILE,
        schema=LPR3_DIAGNOSER_SCHEMA,
        columns_to_keep=[
            "DW_EK_KONTAKT",
            "diagnosekode",
            "diagnosetype",
            "senere_afkraeftet",
            "diagnosekode_parent",
            "diagnosetype_parent",
            "lprindberetningssystem",
        ],
    )


if __name__ == "__main__":
    process_lpr3_diagnoser()