from typing import Any

import polars as pl

from .base import ConfigurableService
from .data_service import DataService


class RegisterService(ConfigurableService):
    def __init__(self, data_service: DataService):
        self.data_service = data_service
        self._config: dict[str, Any] = {}

    def process_lpr_data(self) -> tuple[pl.LazyFrame, pl.LazyFrame]:
        """Process LPR2 and LPR3 data"""
        lpr2 = self.data_service.read_parquet(self._config["lpr2_path"])
        lpr3 = self.data_service.read_parquet(self._config["lpr3_path"])
        return lpr2, lpr3

    def initialize(self) -> None:
        """Initialize service resources"""
        pass

    def shutdown(self) -> None:
        """Clean up service resources"""
        pass

    def check_valid(self) -> bool:
        """Validate service configuration"""
        return True

    def configure(self, config: dict[str, Any]) -> None:
        """Configure the service with provided settings"""
        self._config = config
