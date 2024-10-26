from abc import ABC, abstractmethod
from typing import Any, TypeVar

import polars as pl

from cdef_cohort.services.data_service import DataService
from cdef_cohort.services.event_service import EventService
from cdef_cohort.services.mapping_service import MappingService

T = TypeVar("T")


class BaseProcessor(ABC):
    def __init__(self, data_service: DataService, event_service: EventService, mapping_service: MappingService):
        self.data_service = data_service
        self.event_service = event_service
        self.mapping_service = mapping_service
        self.required_columns: list[str] = []
        self.schema: dict[str, Any] = {}
        self.defaults: dict[str, Any] = {}

    @abstractmethod
    def preprocess(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Preprocess the data"""
        pass

    @abstractmethod
    def process(self, **kwargs: Any) -> T:
        """Process the register data"""
        pass

    def validate(self, df: pl.LazyFrame) -> bool:
        """Validate dataframe schema and contents"""
        try:
            schema = df.collect_schema()
            return all(col in schema.names() for col in self.required_columns)
        except Exception:
            return False
