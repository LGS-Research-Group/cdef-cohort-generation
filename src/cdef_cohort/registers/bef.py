from collections.abc import Callable
from typing import Any

import polars as pl

from cdef_cohort.logging_config import logger
from cdef_cohort.registers.base import BaseProcessor
from cdef_cohort.services.data_service import DataService
from cdef_cohort.services.event_service import EventService
from cdef_cohort.services.mapping_service import MappingService
from cdef_cohort.utils.config import BEF_FILES, BEF_OUT, POPULATION_FILE
from cdef_cohort.utils.types import KwargsType


class BEFProcessor(BaseProcessor):
    BEF_SCHEMA = {
        "AEGTE_ID": pl.Utf8,  # PNR of the spouse
        "ALDER": pl.Int8,  # Age (derived from FOED_DAG)
        "ANTBOERNF": pl.Int8,  # Number of children in the family
        "ANTBOERNH": pl.Int8,  # Number of children in the household
        "ANTPERSF": pl.Int8,  # Number of persons in the family
        "ANTPERSH": pl.Int8,  # Number of persons in the household
        "BOP_VFRA": pl.Date,  # Date of moving in
        "CIVST": pl.Categorical,  # Civil status
        "CPRTJEK": pl.Int8,  # Checksum for CPR/PNR number
        "CPRTYPE": pl.Int8,  # Type of CPR/PNR number
        "E_FAELLE_ID": pl.Utf8,  # PNR of the partner
        "FAMILIE_ID": pl.Utf8,  # Unique family ID
        "FAMILIE_TYPE": pl.Utf8,  # Family type
        "FAR_ID": pl.Utf8,  # PNR of the father
        "FM_MARK": pl.Categorical,  # Family mark
        "FOED_DAG": pl.Date,  # Date of birth
        "HUSTYPE": pl.Categorical,  # Household type
        "IE_TYPE": pl.Utf8,  # Immigration/emigration type
        "KOEN": pl.Utf8,  # Gender
        "KOM": pl.Int8,  # Municipality code
        "MOR_ID": pl.Utf8,  # PNR of the mother
        "OPR_LAND": pl.Utf8,  # Country of origin
        "PLADS": pl.Categorical,  # The person's place in the family
        "PNR": pl.Utf8,  # CPR/PNR number
        "REG": pl.Categorical,  # Region
        "STATSB": pl.Categorical,  # Citizenship
        "VERSION": pl.Utf8,  # Version of the data
    }

    BEF_DEFAULTS = {
        "population_file": POPULATION_FILE,
        "columns_to_keep": [
            "AEGTE_ID",
            "ALDER",
            "ANTBOERNF",
            "ANTBOERNH",
            "ANTPERSF",
            "ANTPERSH",
            "BOP_VFRA",
            "CIVST",
            "E_FAELLE_ID",
            "FAMILIE_ID",
            "FAMILIE_TYPE",
            "FAR_ID",
            "FM_MARK",
            "FOED_DAG",
            "HUSTYPE",
            "IE_TYPE",
            "KOEN",
            "KOM",
            "MOR_ID",
            "OPR_LAND",
            "PLADS",
            "PNR",
            "REG",
            "STATSB",
            "year",
            "month",
        ],
        "date_columns": ["FOED_DAG", "BOP_VFRA"],
        "join_parents_only": False,
        "longitudinal": True,
    }

    def __init__(
        self,
        data_service: DataService,
        event_service: EventService,
        mapping_service: MappingService,
    ):
        super().__init__(data_service, event_service, mapping_service)
        self.schema = self.BEF_SCHEMA
        self.defaults = self.BEF_DEFAULTS
        logger.debug("Initialized BEFProcessor")

    def preprocess(self, df: pl.LazyFrame) -> pl.LazyFrame:
        logger.debug("Preprocessing BEF data")
        return df.with_columns(
            [
                pl.col("FM_MARK").cast(pl.Utf8).pipe(self.mapping_service.apply_mapping, "fm_mark"),
                pl.col("CIVST").cast(pl.Utf8).pipe(self.mapping_service.apply_mapping, "civst"),
                pl.col("HUSTYPE").cast(pl.Utf8).pipe(self.mapping_service.apply_mapping, "hustype"),
                pl.col("PLADS").cast(pl.Utf8).pipe(self.mapping_service.apply_mapping, "plads"),
                pl.col("REG").cast(pl.Utf8).pipe(self.mapping_service.apply_mapping, "reg"),
                pl.col("STATSB").cast(pl.Utf8).pipe(self.mapping_service.apply_mapping, "statsb"),
            ]
        )

    @property
    def process_func(self) -> Callable[..., Any]:
        return self.process

    def process(self, **kwargs: KwargsType) -> None:
        logger.info("Processing BEF data")
        try:
            # Read data
            df = self.data_service.read_parquet(BEF_FILES)

            # Preprocess
            df = self.preprocess(df)

            # Write output
            self.data_service.write_parquet(df, BEF_OUT)
            logger.info("BEF processing completed successfully")

        except Exception as e:
            logger.error(f"Error processing BEF data: {str(e)}")
            raise
