import polars as pl

from cdef_cohort_generation.utils import (
    LPR_DIAG_FILES,
    LPR_DIAG_OUT,
    KwargsType,
    process_register_data,
)

LPR_DIAG_SCHEMA = {
    "C_DIAG": pl.Utf8,  # Diagnosekode
    "C_DIAGTYPE": pl.Utf8,  # Diagnosetype
    "C_TILDIAG": pl.Utf8,  # Tillægsdiagnose
    "LEVERANCEDATO": pl.Date,  # DST leverancedato
    "RECNUM": pl.Utf8,  # LPR-identnummer
    "VERSION": pl.Utf8,  # DST Version
}


LPR_DIAG_DEFAULTS = {
    "population_file": None,
    "columns_to_keep": [
        "RECNUM",
        "C_DIAG",
        "C_TILDIAG",
    ],
    "date_columns": [
        "LEVERANCEDATO",
    ],
}


def process_lpr_diag(**kwargs: KwargsType) -> None:
    process_register_data(
        input_files=LPR_DIAG_FILES,
        output_file=LPR_DIAG_OUT,
        schema=LPR_DIAG_SCHEMA,
        defaults=LPR_DIAG_DEFAULTS,
        **kwargs,
    )


if __name__ == "__main__":
    process_lpr_diag()
