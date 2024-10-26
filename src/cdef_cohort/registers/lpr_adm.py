from collections.abc import Callable
from typing import Any

import polars as pl

from cdef_cohort.logging_config import logger
from cdef_cohort.registers.base import BaseProcessor
from cdef_cohort.services.data_service import DataService
from cdef_cohort.services.event_service import EventService
from cdef_cohort.services.mapping_service import MappingService
from cdef_cohort.utils.config import LPR_ADM_FILES, LPR_ADM_OUT
from cdef_cohort.utils.types import KwargsType


class LPRAdmProcessor(BaseProcessor):
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
    def __init__(
            self,
            data_service: DataService,
            event_service: EventService,
            mapping_service: MappingService,
        ):
            super().__init__(data_service, event_service, mapping_service)
            self.schema = self.LPR_ADM_SCHEMA
            self.defaults = self.LPR_ADM_DEFAULTS
            logger.debug("Initialized LPRAdmProcessor")

    def preprocess(self, df: pl.LazyFrame) -> pl.LazyFrame:
        logger.debug("Preprocessing LPR ADM data")
        return df

    @property
    def process_func(self) -> Callable[..., Any]:
        return self.process

    def process(self, **kwargs: KwargsType) -> None:
        logger.info("Processing LPR ADM data")
        try:
            df = self.data_service.read_parquet(LPR_ADM_FILES)
            df = self.preprocess(df)
            self.data_service.write_parquet(df, LPR_ADM_OUT)
            logger.info("LPR ADM processing completed successfully")
        except Exception as e:
            logger.error(f"Error processing LPR ADM data: {str(e)}")
            raise
