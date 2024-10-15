import polars as pl

from cdef_cohort_generation.utils import (
    LPR_BES_FILES,
    LPR_BES_OUT,
    KwargsType,
    process_register_data,
)

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


def process_lpr_bes(**kwargs: KwargsType) -> None:
    process_register_data(
        input_files=LPR_BES_FILES,
        output_file=LPR_BES_OUT,
        schema=LPR_BES_SCHEMA,
        defaults=LPR_BES_DEFAULTS,
        **kwargs,
    )


if __name__ == "__main__":
    process_lpr_bes()
