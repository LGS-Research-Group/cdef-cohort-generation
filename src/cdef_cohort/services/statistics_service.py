from typing import Any

import polars as pl

from cdef_cohort.logging_config import logger
from cdef_cohort.models.statistics import (
    CategoricalStatistic,
    NumericStatistic,
    StatisticType,
    TemporalStatistic,
)

from .base import ConfigurableService


class StatisticsService(ConfigurableService):
    def __init__(self):
        self._config: dict[str, Any] = {}

    def initialize(self) -> None:
        """Initialize service resources"""
        if not self.check_valid():
            raise ValueError("Invalid configuration")

    def shutdown(self) -> None:
        """Clean up service resources"""
        pass  # No resources to clean up

    def configure(self, config: dict[str, Any]) -> None:
        """Configure the service with provided settings"""
        self._config = config
        if not self.check_valid():
            raise ValueError("Invalid configuration")

    def check_valid(self) -> bool:
        """Validate service configuration"""
        required_configs = ["output_path", "domains"]
        return all(key in self._config for key in required_configs)

    def calculate_numeric_statistics(self, df: pl.LazyFrame, column: str) -> NumericStatistic:
        """Calculate numeric statistics for a column"""
        try:
            stats = df.select(
                [
                    pl.col(column).count().alias("count"),
                    pl.col(column).mean().alias("mean"),
                    pl.col(column).std().alias("std"),
                    pl.col(column).median().alias("median"),
                    pl.col(column).quantile(0.25).alias("q1"),
                    pl.col(column).quantile(0.75).alias("q3"),
                    pl.col(column).min().alias("min"),
                    pl.col(column).max().alias("max"),
                    pl.col(column).is_null().sum().alias("missing"),
                ]
            ).collect()

            return NumericStatistic(
                count=stats["count"][0],
                mean=stats["mean"][0],
                std=stats["std"][0],
                median=stats["median"][0],
                q1=stats["q1"][0],
                q3=stats["q3"][0],
                min=stats["min"][0],
                max=stats["max"][0],
                missing=stats["missing"][0],
            )
        except Exception as e:
            logger.error(f"Error calculating numeric statistics for {column}: {e}")
            raise

    def calculate_categorical_statistics(self, df: pl.LazyFrame, column: str) -> CategoricalStatistic:
        """Calculate categorical statistics for a column"""
        try:
            total = df.select(pl.count()).collect().item()

            value_counts = df.group_by(column).agg(pl.count().alias("count")).sort("count", descending=True).collect()

            categories = {
                str(val): count
                for val, count in zip(
                    value_counts[column].to_list(),
                    value_counts["count"].to_list(),
                    strict=False,
                )
            }

            percentages = {str(val): count / total * 100 for val, count in categories.items()}

            missing = df.select(pl.col(column).is_null().sum()).collect().item()

            return CategoricalStatistic(categories=categories, percentages=percentages, missing=missing, total=total)
        except Exception as e:
            logger.error(f"Error calculating categorical statistics for {column}: {e}")
            raise

    def calculate_temporal_statistics(self, df: pl.LazyFrame, column: str) -> TemporalStatistic:
        """Calculate temporal statistics for a column"""
        try:
            stats = df.select(
                [
                    pl.col(column).count().alias("count"),
                    pl.col(column).min().alias("min"),
                    pl.col(column).max().alias("max"),
                    pl.col(column).is_null().sum().alias("missing"),
                ]
            ).collect()

            return TemporalStatistic(
                count=stats["count"][0],
                min=str(stats["min"][0]),
                max=str(stats["max"][0]),
                missing=stats["missing"][0],
            )
        except Exception as e:
            logger.error(f"Error calculating temporal statistics for {column}: {e}")
            raise

    def get_column_type(self, df: pl.LazyFrame, column: str) -> StatisticType:
        """Determine the type of statistics to calculate for a column"""
        schema = df.collect_schema()
        dtype = schema[column]

        if dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8]:
            return StatisticType.NUMERIC
        elif dtype in [pl.Date, pl.Datetime]:
            return StatisticType.TEMPORAL
        else:
            return StatisticType.CATEGORICAL
