import polars as pl

from cdef_cohort_generation.utils import (
    LPR_DIAG_FILES,
    LPR_DIAG_OUT,
    POPULATION_FILE,
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


def process_lpr_diag(columns_to_keep: list[str] | None = None, **kwargs: KwargsType) -> None:
    default_columns = ["C_DIAG", "C_DIAGTYPE", "RECNUM"]
    columns = columns_to_keep if columns_to_keep is not None else default_columns
    process_register_data(
        input_files=LPR_DIAG_FILES,
        output_file=LPR_DIAG_OUT,
        population_file=POPULATION_FILE,
        schema=LPR_DIAG_SCHEMA,
        date_columns=[
            "LEVERANCEDATO",
        ],
        columns_to_keep=columns,
        **kwargs,
    )
