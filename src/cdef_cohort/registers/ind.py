from collections.abc import Callable
from typing import Any

import polars as pl

from cdef_cohort.logging_config import logger
from cdef_cohort.registers.base import BaseProcessor
from cdef_cohort.services.data_service import DataService
from cdef_cohort.services.event_service import EventService
from cdef_cohort.services.mapping_service import MappingService
from cdef_cohort.utils.config import IND_FILES, IND_OUT, POPULATION_FILE
from cdef_cohort.utils.types import KwargsType


class INDProcessor(BaseProcessor):
    IND_SCHEMA = {
        "BESKST13": pl.Int8,
        "CPRTJEK": pl.Utf8,
        "CPRTYPE": pl.Utf8,
        "LOENMV_13": pl.Float64,
        "PERINDKIALT_13": pl.Float64,
        "PNR": pl.Utf8,
        "PRE_SOCIO": pl.Int8,
        "VERSION": pl.Utf8,
    }

    IND_DEFAULTS = {
        "population_file": POPULATION_FILE,
        "columns_to_keep": ["PNR", "BESKST13", "LOENMV_13", "PERINDKIALT_13", "PRE_SOCIO", "year"],
        "join_parents_only": True,
        "longitudinal": True,
    }

    def __init__(
        self,
        data_service: DataService,
        event_service: EventService,
        mapping_service: MappingService,
    ):
        super().__init__(data_service, event_service, mapping_service)
        self.schema = self.IND_SCHEMA
        self.defaults = self.IND_DEFAULTS
        logger.debug("Initialized INDProcessor")

    @property
    def process_func(self) -> Callable[..., Any]:
        return self.process

    def preprocess(self, df: pl.LazyFrame) -> pl.LazyFrame:
        logger.debug("Preprocessing IND data")
        # Add any preprocessing specific to IND
        return df

    def process(self, **kwargs: KwargsType) -> None:
        logger.info("Processing IND data")
        try:
            # Read data
            df = self.data_service.read_parquet(IND_FILES)

            # Preprocess
            df = self.preprocess(df)

            # Write output
            self.data_service.write_parquet(df, IND_OUT)
            logger.info("IND processing completed successfully")

        except Exception as e:
            logger.error(f"Error processing IND data: {str(e)}")
            raise
