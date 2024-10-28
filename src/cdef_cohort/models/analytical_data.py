from pathlib import Path
from typing import Literal

from pydantic import BaseModel


class DataDomain(BaseModel):
    name: str
    description: str
    sources: list[str]
    temporal: bool = False


class AnalyticalDataConfig(BaseModel):
    base_path: Path
    zones: dict[Literal["static", "longitudinal", "family", "derived"], Path]
    domains: dict[str, DataDomain] = {
        "demographics": DataDomain(
            name="demographics",
            description="Basic demographic information",
            sources=["bef_longitudinal"],
            temporal=True,
        ),
        "education": DataDomain(
            name="education",
            description="Educational history and achievements",
            sources=["uddf_longitudinal"],
            temporal=True,
        ),
        "income": DataDomain(
            name="income",
            description="Income and financial data",
            sources=["ind_longitudinal"],
            temporal=True,
        ),
        "employment": DataDomain(
            name="employment",
            description="Employment history and status",
            sources=["akm_longitudinal"],
            temporal=True,
        ),
    }
