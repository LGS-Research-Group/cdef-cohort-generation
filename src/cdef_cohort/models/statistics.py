from enum import Enum
from typing import Any

from pydantic import BaseModel


class StatisticType(str, Enum):
    NUMERIC = "numeric"
    CATEGORICAL = "categorical"
    TEMPORAL = "temporal"
    CUSTOM = "custom"

class NumericStatistic(BaseModel):
    count: int
    mean: float
    std: float
    median: float
    q1: float
    q3: float
    min: float
    max: float
    missing: int

class CategoricalStatistic(BaseModel):
    categories: dict[str, int]
    percentages: dict[str, float]
    missing: int
    total: int

class TemporalStatistic(BaseModel):
    count: int
    min: str
    max: str
    missing: int

class CustomStatistic(BaseModel):
    name: str
    value: Any
    description: str | None = None

class DomainStatistics(BaseModel):
    domain: str
    year: int | None
    numeric_stats: dict[str, NumericStatistic] = {}
    categorical_stats: dict[str, CategoricalStatistic] = {}
    temporal_stats: dict[str, TemporalStatistic] = {}
    custom_stats: dict[str, CustomStatistic] = {}

class StatisticsResult(BaseModel):
    static: list[DomainStatistics]
    family: list[DomainStatistics]
    longitudinal: dict[int, list[DomainStatistics]]
