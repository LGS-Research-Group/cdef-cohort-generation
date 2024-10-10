import polars as pl

from cdef_cohort_generation.config import LPR3_KONTAKTER_FILES, LPR3_KONTAKTER_OUT, POPULATION_FILE
from cdef_cohort_generation.utils import process_register_data

LPR3_KONTAKTER_SCHEMA = {
    "SORENHED_IND": pl.Utf8,
    "SORENHED_HEN": pl.Utf8,
    "SORENHED_ANS": pl.Utf8,
    "DW_EK_KONTAKT": pl.Utf8,
    "DW_EK_FORLOEB": pl.Utf8,
    "CPR": pl.Utf8,
    "dato_start": pl.Date,
    "tidspunkt_start": pl.Time,
    "dato_slut": pl.Date,
    "tidspunkt_slut": pl.Time,
    "aktionsdiagnose": pl.Utf8,
    "kontaktaarsag": pl.Utf8,
    "prioritet": pl.Utf8,
    "kontakttype": pl.Utf8,
    "henvisningsaarsag": pl.Utf8,
    "henvisningsmaade": pl.Utf8,
    "dato_behandling_start": pl.Date,
    "tidspunkt_behandling_start": pl.Time,
    "dato_indberetning": pl.Date,
    "lprindberetningssytem": pl.Utf8,
}


def process_lpr3_kontakter() -> None:
    process_register_data(
        input_files=LPR3_KONTAKTER_FILES,
        output_file=LPR3_KONTAKTER_OUT,
        population_file=POPULATION_FILE,
        schema=LPR3_KONTAKTER_SCHEMA,
        date_columns=["dato_slut", "dato_start", "dato_behandling_start", "dato_indberetning"],
        columns_to_keep=[
            "D_AMBDTO",
            "RECNUM",
        ],
    )
