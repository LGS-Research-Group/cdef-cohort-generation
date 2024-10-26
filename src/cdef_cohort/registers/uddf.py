import polars as pl

from cdef_cohort.logging_config import logger
from cdef_cohort.registers.base import BaseProcessor
from cdef_cohort.services.data_service import DataService
from cdef_cohort.services.event_service import EventService
from cdef_cohort.services.mapping_service import MappingService
from cdef_cohort.utils.config import POPULATION_FILE, UDDF_FILES, UDDF_OUT
from cdef_cohort.utils.types import KwargsType


class UDDFProcessor(BaseProcessor):
    UDDF_SCHEMA = {
        "PNR": pl.Utf8,
        "CPRTJEK": pl.Utf8,
        "CPRTYPE": pl.Utf8,
        "HFAUDD": pl.Utf8,
        "HF_KILDE": pl.Utf8,
        "HF_VFRA": pl.Utf8,
        "HF_VTIL": pl.Utf8,
        "INSTNR": pl.Int8,
        "VERSION": pl.Utf8,
    }

    UDDF_DEFAULTS = {
        "population_file": POPULATION_FILE,
        "columns_to_keep": ["PNR", "HFAUDD", "HF_KILDE", "HF_VFRA", "INSTNR"],
        "date_columns": ["HF_VFRA", "HF_VTIL"],
        "join_parents_only": True,
        "register_name": "UDDF",
        "longitudinal": True,
    }

    def __init__(
        self,
        data_service: DataService,
        event_service: EventService,
        mapping_service: MappingService,
    ):
        super().__init__(data_service, event_service, mapping_service)
        self.schema = self.UDDF_SCHEMA
        self.defaults = self.UDDF_DEFAULTS
        logger.debug("Initialized UDDFProcessor")

    def preprocess(self, df: pl.LazyFrame) -> pl.LazyFrame:
        logger.debug("Preprocessing UDDF data")
        # Add any UDDF-specific preprocessing
        return df

    def process(self, **kwargs: KwargsType) -> None:
        logger.info("Processing UDDF data")
        try:
            # Read data
            df = self.data_service.read_parquet(UDDF_FILES)

            # Preprocess
            df = self.preprocess(df)

            # Write output
            self.data_service.write_parquet(df, UDDF_OUT)
            logger.info("UDDF processing completed successfully")

        except Exception as e:
            logger.error(f"Error processing UDDF data: {str(e)}")
            raise
