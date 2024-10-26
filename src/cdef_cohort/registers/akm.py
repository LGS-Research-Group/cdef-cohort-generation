from collections.abc import Callable
from typing import Any

import polars as pl

from cdef_cohort.registers.base import BaseProcessor
from cdef_cohort.utils.config import AKM_FILES, AKM_OUT, POPULATION_FILE


class AKMProcessor(BaseProcessor):
    def __init__(self, data_service, event_service, mapping_service):
        super().__init__(data_service, event_service, mapping_service)
        self.schema = {
            "PNR": pl.Utf8,
            "SOCIO": pl.Int8,
            "SOCIO02": pl.Int8,
            "SOCIO13": pl.Categorical,
            "CPRTJEK": pl.Utf8,
            "CPRTYPE": pl.Utf8,
            "VERSION": pl.Utf8,
            "SENR": pl.Utf8,
        }

        self.defaults = {
            "population_file": POPULATION_FILE,
            "columns_to_keep": ["PNR", "SOCIO13", "SENR", "year"],
            "join_parents_only": True,
            "longitudinal": False,
        }

    def preprocess(self, df: pl.LazyFrame) -> pl.LazyFrame:
        return df.with_columns(pl.col("SOCIO13").cast(pl.Utf8).pipe(self.mapping_service.apply_mapping, "socio13"))

    def process(self, **kwargs) -> None:
        # Read data using data service
        df = self.data_service.read_parquet(AKM_FILES)

        # Preprocess
        df = self.preprocess(df)

        # Write output using data service
        self.data_service.write_parquet(df, AKM_OUT)

    @property
    def process_func(self) -> Callable[..., Any]:
        return self.process
