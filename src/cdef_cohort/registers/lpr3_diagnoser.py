from collections.abc import Callable
from typing import Any

import polars as pl

from cdef_cohort.logging_config import logger
from cdef_cohort.registers.base import BaseProcessor
from cdef_cohort.services.data_service import DataService
from cdef_cohort.services.event_service import EventService
from cdef_cohort.services.mapping_service import MappingService
from cdef_cohort.utils.config import (
    LPR3_DIAGNOSER_FILES,
    LPR3_DIAGNOSER_OUT,
)
from cdef_cohort.utils.types import KwargsType


class LPR3DiagnoserProcessor(BaseProcessor):
    LPR3_DIAGNOSER_SCHEMA = {
        "DW_EK_KONTAKT": pl.Utf8,
        "diagnosekode": pl.Utf8,
        "diagnosetype": pl.Utf8,
        "senere_afkraeftet": pl.Utf8,
        "diagnosekode_parent": pl.Utf8,
        "diagnosetype_parent": pl.Utf8,
        "lprindberetningssystem": pl.Utf8,
    }

    LPR3_DIAGNOSER_DEFAULTS = {
        "population_file": None,
        "columns_to_keep": ["DW_EK_KONTAKT", "diagnosekode"],
    }

    def __init__(
        self,
        data_service: DataService,
        event_service: EventService,
        mapping_service: MappingService,
    ):
        super().__init__(data_service, event_service, mapping_service)
        self.schema = self.LPR3_DIAGNOSER_SCHEMA
        self.defaults = self.LPR3_DIAGNOSER_DEFAULTS
        logger.debug("Initialized LPR3DiagnoserProcessor")

    def preprocess(self, df: pl.LazyFrame) -> pl.LazyFrame:
        logger.debug("Preprocessing LPR3 Diagnoser data")
        return df

    def process(self, **kwargs: KwargsType) -> None:
        logger.info("Processing LPR3 Diagnoser data")
        try:
            df = self.data_service.read_parquet(LPR3_DIAGNOSER_FILES)
            df = self.preprocess(df)
            self.data_service.write_parquet(df, LPR3_DIAGNOSER_OUT)
            logger.info("LPR3 Diagnoser processing completed successfully")
        except Exception as e:
            logger.error(f"Error processing LPR3 Diagnoser data: {str(e)}")
            raise

    @property
    def process_func(self) -> Callable[..., Any]:
        return self.process
