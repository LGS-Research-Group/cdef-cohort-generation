from collections.abc import Callable
from typing import Any

import polars as pl

from cdef_cohort.logging_config import logger
from cdef_cohort.registers.base import BaseProcessor
from cdef_cohort.services.data_service import DataService
from cdef_cohort.services.event_service import EventService
from cdef_cohort.services.mapping_service import MappingService
from cdef_cohort.utils.config import IDAN_FILES, IDAN_OUT, POPULATION_FILE
from cdef_cohort.utils.types import KwargsType


class IDANProcessor(BaseProcessor):
    IDAN_SCHEMA = {
        "ARBGNR": pl.Utf8,  # Arbejdsgivernummer
        "ARBNR": pl.Utf8,  # Arbejdsstedsnummer
        "CPRTJEK": pl.Utf8,
        "CPRTYPE": pl.Utf8,
        "CVRNR": pl.Utf8,
        "JOBKAT": pl.Int8,
        "JOBLON": pl.Float64,
        "LBNR": pl.Utf8,
        "PNR": pl.Utf8,
        "STILL": pl.Utf8,
        "TILKNYT": pl.Int8,
    }

    IDAN_DEFAULTS = {
        "population_file": POPULATION_FILE,
        "columns_to_keep": [
            "PNR",
            "ARBGNR",
            "ARBNR",
            "CVRNR",
            "JOBKAT",
            "JOBLON",
            "LBNR",
            "STILL",
            "TILKNYT",
            "year",
        ],
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
        self.schema = self.IDAN_SCHEMA
        self.defaults = self.IDAN_DEFAULTS
        logger.debug("Initialized IDANProcessor")

    @property
    def process_func(self) -> Callable[..., Any]:
        return self.process

    def preprocess(self, df: pl.LazyFrame) -> pl.LazyFrame:
        logger.debug("Preprocessing IDAN data")
        return df.with_columns(
            [
                pl.col("JOBKAT").cast(pl.Utf8).pipe(self.mapping_service.apply_mapping, "jobkat"),
                pl.col("TILKNYT").cast(pl.Utf8).pipe(self.mapping_service.apply_mapping, "tilknyt"),
            ]
        )

    def process(self, **kwargs: KwargsType) -> None:
        logger.info("Processing IDAN data")
        try:
            # Read data
            df = self.data_service.read_parquet(IDAN_FILES)

            # Preprocess
            df = self.preprocess(df)

            # Write output
            self.data_service.write_parquet(df, IDAN_OUT)
            logger.info("IDAN processing completed successfully")

        except Exception as e:
            logger.error(f"Error processing IDAN data: {str(e)}")
            raise
