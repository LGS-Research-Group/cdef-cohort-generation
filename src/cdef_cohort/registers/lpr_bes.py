from collections.abc import Callable
from typing import Any

import polars as pl

from cdef_cohort.logging_config import logger
from cdef_cohort.registers.base import BaseProcessor
from cdef_cohort.services.data_service import DataService
from cdef_cohort.services.event_service import EventService
from cdef_cohort.services.mapping_service import MappingService
from cdef_cohort.utils.config import (
    LPR_BES_FILES,
    LPR_BES_OUT,
)
from cdef_cohort.utils.types import KwargsType


class LPRBesProcessor(BaseProcessor):
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

    def __init__(
        self,
        data_service: DataService,
        event_service: EventService,
        mapping_service: MappingService,
    ):
        super().__init__(data_service, event_service, mapping_service)
        self.schema = self.LPR_BES_SCHEMA
        self.defaults = self.LPR_BES_DEFAULTS
        logger.debug("Initialized LPRBesProcessor")

    def preprocess(self, df: pl.LazyFrame) -> pl.LazyFrame:
        logger.debug("Preprocessing LPR BES data")
        return df

    @property
    def process_func(self) -> Callable[..., Any]:
        return self.process

    def process(self, **kwargs: KwargsType) -> None:
        logger.info("Processing LPR BES data")
        try:
            df = self.data_service.read_parquet(LPR_BES_FILES)
            df = self.preprocess(df)
            self.data_service.write_parquet(df, LPR_BES_OUT)
            logger.info("LPR BES processing completed successfully")
        except Exception as e:
            logger.error(f"Error processing LPR BES data: {str(e)}")
            raise
