from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field


class StatisticsConfig(BaseModel):
    """Configuration for statistics calculation"""

    numeric_columns: dict[str, list[str]] = Field(default_factory=dict)
    categorical_columns: dict[str, list[str]] = Field(default_factory=dict)
    temporal_columns: dict[str, list[str]] = Field(default_factory=dict)
    custom_statistics: dict[str, dict[str, Any]] = Field(default_factory=dict)


class DataDomain(BaseModel):
    name: str
    description: str
    sources: list[str]
    temporal: bool = False
    statistics: StatisticsConfig = Field(default_factory=StatisticsConfig)


class AnalyticalDataConfig(BaseModel):
    base_path: Path
    zones: dict[Literal["static", "longitudinal", "family", "derived"], Path]
    domains: dict[str, DataDomain] = {
        "demographics": DataDomain(
            name="demographics",
            description="Basic demographic information",
            sources=["bef_longitudinal"],
            temporal=True,
            statistics=StatisticsConfig(
                numeric_columns={"longitudinal": ["household_size"]},
                categorical_columns={"longitudinal": ["municipality", "region"]},
                temporal_columns={"static": ["birth_date"]},
            ),
        ),
        "education": DataDomain(
            name="education",
            description="Educational history and achievements",
            sources=["uddf_longitudinal"],
            temporal=True,
            statistics=StatisticsConfig(
                numeric_columns={"longitudinal": ["education_level"]},
                categorical_columns={"longitudinal": ["education_field"]},
            ),
        ),
        "income": DataDomain(
            name="income",
            description="Income and financial data",
            sources=["ind_longitudinal"],
            temporal=True,
            statistics=StatisticsConfig(numeric_columns={"longitudinal": ["annual_income", "disposable_income"]}),
        ),
        "employment": DataDomain(
            name="employment",
            description="Employment history and status",
            sources=["akm_longitudinal"],
            temporal=True,
            statistics=StatisticsConfig(categorical_columns={"longitudinal": ["employment_status", "sector"]}),
        ),
    }
