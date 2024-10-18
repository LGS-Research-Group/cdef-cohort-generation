import polars as pl

from cdef_cohort_builder.logging_config import logger
from cdef_cohort_builder.registers.generic import process_register_data
from cdef_cohort_builder.utils.config import (
    LPR_ADM_FILES,
    LPR_ADM_OUT,
)
from cdef_cohort_builder.utils.logging_decorator import log_processing
from cdef_cohort_builder.utils.types import KwargsType

LPR_ADM_SCHEMA = {
    "PNR": pl.Utf8,  # Personnummer
    "C_ADIAG": pl.Utf8,  # Aktionsdiagnose
    "C_AFD": pl.Utf8,  # Afdelingskode
    "C_HAFD": pl.Utf8,  # Henvisende afdeling
    "C_HENM": pl.Utf8,  # Henvisningsmåde
    "C_HSGH": pl.Utf8,  # Henvisende sygehus
    "C_INDM": pl.Utf8,  # Indlæggelsesmåde
    "C_KOM": pl.Utf8,  # Kommune
    "C_KONTAARS": pl.Utf8,  # Kontaktårsag
    "C_PATTYPE": pl.Utf8,  # Patienttype
    "C_SGH": pl.Utf8,  # Sygehus
    "C_SPEC": pl.Utf8,  # Specialekode
    "C_UDM": pl.Utf8,  # Udskrivningsmåde
    "CPRTJEK": pl.Utf8,  # CPR-tjek
    "CPRTYPE": pl.Utf8,  # CPR-type
    "D_HENDTO": pl.Date,  # Henvisningsdato
    "D_INDDTO": pl.Date,  # Indlæggelsesdato
    "D_UDDTO": pl.Date,  # Udskrivningsdato
    "K_AFD": pl.Utf8,  # Afdelingskode
    "RECNUM": pl.Utf8,  # LPR-identnummer
    "V_ALDDG": pl.Int32,  # Alder i dage ved kontaktens start
    "V_ALDER": pl.Int32,  # Alder i år ved kontaktens start
    "V_INDMINUT": pl.Int32,  # Indlæggelsminut
    "V_INDTIME": pl.Int32,  # Indlæggelsestidspunkt
    "V_SENGDAGE": pl.Int32,  # Sengedage
    "V_UDTIME": pl.Int32,  # Udskrivningstime
    "VERSION": pl.Utf8,  # DST Version
}


LPR_ADM_DEFAULTS = {
    "population_file": None,
    "columns_to_keep": [
        "PNR",
        "C_ADIAG",
        "D_INDDTO",
        "RECNUM",
    ],
    "date_columns": [
        "D_HENDTO",
        "D_INDDTO",
        "D_UDDTO",
    ],
}


logger.debug(f"LPR_ADM_SCHEMA: {LPR_ADM_SCHEMA}")
logger.debug(f"LPR_ADM_DEFAULTS: {LPR_ADM_DEFAULTS}")


@log_processing
def process_lpr_adm(**kwargs: KwargsType) -> None:
    process_register_data(
        input_files=LPR_ADM_FILES,
        output_file=LPR_ADM_OUT,
        schema=LPR_ADM_SCHEMA,
        defaults=LPR_ADM_DEFAULTS,
        register_name="LPR_ADM",
        **kwargs,
    )


def main() -> None:
    process_lpr_adm()


if __name__ == "__main__":
    main()
