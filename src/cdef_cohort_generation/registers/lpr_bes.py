import polars as pl

from cdef_cohort_generation.config import LPR_BES_FILES, LPR_BES_OUT, POPULATION_FILE
from cdef_cohort_generation.utils import process_register_data

LPR_BES_SCHEMA = {
    "D_AMBDTO": pl.Date,  # Dato for ambulantbesøg
    "LEVERANCEDATO": pl.Date,  # DST leverancedato
    "RECNUM": pl.Utf8,  # LPR-identnummer
    "VERSION": pl.Utf8,  # DST Version
}


def process_lpr_bes(columns_to_keep: list[str] | None = None) -> None:
    default_columns = ["D_AMBDTO", "RECNUM"]
    columns = columns_to_keep if columns_to_keep is not None else default_columns
    process_register_data(
        input_files=LPR_BES_FILES,
        output_file=LPR_BES_OUT,
        population_file=POPULATION_FILE,
        schema=LPR_BES_SCHEMA,
        date_columns=["D_AMBDTO", "LEVERANCEDATO"],
        columns_to_keep=columns,
    )
