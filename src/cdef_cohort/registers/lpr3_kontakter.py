from collections.abc import Callable
from typing import Any

import polars as pl

from cdef_cohort.logging_config import logger
from cdef_cohort.registers.base import BaseProcessor
from cdef_cohort.services.data_service import DataService
from cdef_cohort.services.event_service import EventService
from cdef_cohort.services.mapping_service import MappingService
from cdef_cohort.utils.config import (
    LPR3_KONTAKTER_FILES,
    LPR3_KONTAKTER_OUT,
)
from cdef_cohort.utils.types import KwargsType


class LPR3KontakterProcessor(BaseProcessor):
    LPR3_KONTAKTER_SCHEMA = {
        "SORENHED_IND": pl.Utf8,
        "SORENHED_HEN": pl.Utf8,
        "SORENHED_ANS": pl.Utf8,
        "DW_EK_KONTAKT": pl.Utf8,
        "DW_EK_FORLOEB": pl.Utf8,
        "CPR": pl.Utf8,
        "dato_start": pl.Utf8,
        "tidspunkt_start": pl.Utf8,
        "dato_slut": pl.Utf8,
        "tidspunkt_slut": pl.Utf8,
        "aktionsdiagnose": pl.Utf8,
        "kontaktaarsag": pl.Utf8,
        "prioritet": pl.Utf8,
        "kontakttype": pl.Utf8,
        "henvisningsaarsag": pl.Utf8,
        "henvisningsmaade": pl.Utf8,
        "dato_behandling_start": pl.Utf8,
        "tidspunkt_behandling_start": pl.Utf8,
        "dato_indberetning": pl.Utf8,
        "lprindberetningssytem": pl.Utf8,
    }

    LPR3_KONTAKTER_DEFAULTS = {
        "population_file": None,
        "columns_to_keep": ["DW_EK_KONTAKT", "CPR", "dato_start", "aktionsdiagnose", "dato_slut"],
        "date_columns": ["dato_slut", "dato_start", "dato_behandling_start", "dato_indberetning"],
        "register_name": "LPR3_KONTAKTER",
    }

    def __init__(
        self,
        data_service: DataService,
        event_service: EventService,
        mapping_service: MappingService,
    ):
        super().__init__(data_service, event_service, mapping_service)
        self.schema = self.LPR3_KONTAKTER_SCHEMA
        self.defaults = self.LPR3_KONTAKTER_DEFAULTS
        logger.debug("Initialized LPR3KontakterProcessor")

    def preprocess(self, df: pl.LazyFrame) -> pl.LazyFrame:
        logger.debug("Preprocessing LPR3 Kontakter data")
        return df

    @property
    def process_func(self) -> Callable[..., Any]:
        return self.process

    def process(self, **kwargs: KwargsType) -> None:
        logger.info("Processing LPR3 Kontakter data")
        try:
            df = self.data_service.read_parquet(LPR3_KONTAKTER_FILES)
            df = self.preprocess(df)
            self.data_service.write_parquet(df, LPR3_KONTAKTER_OUT)
            logger.info("LPR3 Kontakter processing completed successfully")
        except Exception as e:
            logger.error(f"Error processing LPR3 Kontakter data: {str(e)}")
            raise
