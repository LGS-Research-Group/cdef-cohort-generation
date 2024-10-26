from collections.abc import Callable
from typing import Any

import polars as pl

from cdef_cohort.logging_config import logger
from cdef_cohort.registers.base import BaseProcessor
from cdef_cohort.services.data_service import DataService
from cdef_cohort.services.event_service import EventService
from cdef_cohort.services.mapping_service import MappingService
from cdef_cohort.utils.config import (
    LPR_DIAG_FILES,
    LPR_DIAG_OUT,
)
from cdef_cohort.utils.types import KwargsType


class LPRDiagProcessor(BaseProcessor):
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

    def __init__(
        self,
        data_service: DataService,
        event_service: EventService,
        mapping_service: MappingService,
    ):
        super().__init__(data_service, event_service, mapping_service)
        self.schema = self.LPR_DIAG_SCHEMA
        self.defaults = self.LPR_DIAG_DEFAULTS
        logger.debug("Initialized LPRDiagProcessor")

    def preprocess(self, df: pl.LazyFrame) -> pl.LazyFrame:
        logger.debug("Preprocessing LPR DIAG data")
        return df

    @property
    def process_func(self) -> Callable[..., Any]:
        return self.process

    def process(self, **kwargs: KwargsType) -> None:
        logger.info("Processing LPR DIAG data")
        try:
            df = self.data_service.read_parquet(LPR_DIAG_FILES)
            df = self.preprocess(df)
            self.data_service.write_parquet(df, LPR_DIAG_OUT)
            logger.info("LPR DIAG processing completed successfully")
        except Exception as e:
            logger.error(f"Error processing LPR DIAG data: {str(e)}")
            raise
