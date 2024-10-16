import importlib.resources as pkg_resources
from pathlib import Path
from typing import Any

import polars as pl
from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Basic paths
    BASE_DIR: Path = Path("/Users/tobiaskragholm/dev/TEST_RUN")
    REGISTER_BASE_DIR: Path = Path("/Users/tobiaskragholm/dev/TEST_RUN/registers")

    # Constants
    PARQUETS: str = "*.parquet"
    BIRTH_INCLUSION_START_YEAR: int = 1995
    BIRTH_INCLUSION_END_YEAR: int = 2020

    EVENT_DEFINITIONS: dict[str, Any] = {
        "father_education_change": (pl.col("FAR_EDU_LVL").shift() != pl.col("FAR_EDU_LVL")),
        "mother_education_change": (pl.col("MOR_EDU_LVL").shift() != pl.col("MOR_EDU_LVL")),
        "father_income_change": (pl.col("FAR_PERINDKIALT_13").cast(pl.Float64).diff() != 0),
        "mother_income_change": (pl.col("MOR_PERINDKIALT_13").cast(pl.Float64).diff() != 0),
        "municipality_change": (pl.col("KOM").shift() != pl.col("KOM")),
    }

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="CDEF_",
        env_nested_delimiter="__",
        frozen=True,
    )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def DATA_DIR(self) -> Path:
        return self.BASE_DIR / "data"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def POPULATION_FILE(self) -> Path:
        return self.DATA_DIR / "population.parquet"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def STATIC_COHORT(self) -> Path:
        return self.DATA_DIR / "static_cohort.parquet"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def COHORT_FILE(self) -> Path:
        return self.DATA_DIR / "cohort.parquet"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def ISCED_FILE(self) -> Path:
        return self.DATA_DIR / "isced.parquet"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def ICD_FILE(self) -> Path:
        return self.DATA_DIR / "icd10dict.csv"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def BEF_FILES(self) -> Path:
        return self.REGISTER_BASE_DIR / "bef" / self.PARQUETS

    @computed_field  # type: ignore[prop-decorator]
    @property
    def UDDF_FILES(self) -> Path:
        return self.REGISTER_BASE_DIR / "uddf" / self.PARQUETS

    @computed_field  # type: ignore[prop-decorator]
    @property
    def LPR_ADM_FILES(self) -> Path:
        return self.REGISTER_BASE_DIR / "lpr_adm" / self.PARQUETS

    @computed_field  # type: ignore[prop-decorator]
    @property
    def LPR_DIAG_FILES(self) -> Path:
        return self.REGISTER_BASE_DIR / "lpr_diag" / self.PARQUETS

    @computed_field  # type: ignore[prop-decorator]
    @property
    def LPR_BES_FILES(self) -> Path:
        return self.REGISTER_BASE_DIR / "lpr_bes" / self.PARQUETS

    @computed_field  # type: ignore[prop-decorator]
    @property
    def LPR3_DIAGNOSER_FILES(self) -> Path:
        return self.REGISTER_BASE_DIR / "lpr3_diagnoser" / self.PARQUETS

    @computed_field  # type: ignore[prop-decorator]
    @property
    def LPR3_KONTAKTER_FILES(self) -> Path:
        return self.REGISTER_BASE_DIR / "lpr3_kontakter" / self.PARQUETS

    @computed_field  # type: ignore[prop-decorator]
    @property
    def AKM_FILES(self) -> Path:
        return self.REGISTER_BASE_DIR / "akm" / self.PARQUETS

    @computed_field  # type: ignore[prop-decorator]
    @property
    def IDAN_FILES(self) -> Path:
        return self.REGISTER_BASE_DIR / "idan" / self.PARQUETS

    @computed_field  # type: ignore[prop-decorator]
    @property
    def IND_FILES(self) -> Path:
        return self.REGISTER_BASE_DIR / "ind" / self.PARQUETS

    @computed_field  # type: ignore[prop-decorator]
    @property
    def BEF_OUT(self) -> Path:
        return self.DATA_DIR / "bef" / "bef.parquet"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def UDDF_OUT(self) -> Path:
        return self.DATA_DIR / "uddf" / "uddf.parquet"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def LPR_ADM_OUT(self) -> Path:
        return self.DATA_DIR / "lpr_adm" / "lpr_adm.parquet"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def AKM_OUT(self) -> Path:
        return self.DATA_DIR / "akm" / "akm.parquet"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def IDAN_OUT(self) -> Path:
        return self.DATA_DIR / "idan" / "idan.parquet"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def IND_OUT(self) -> Path:
        return self.DATA_DIR / "ind" / "ind.parquet"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def LPR_DIAG_OUT(self) -> Path:
        return self.DATA_DIR / "lpr_diag" / "lpr_diag.parquet"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def LPR_BES_OUT(self) -> Path:
        return self.DATA_DIR / "lpr_bes" / "lpr_bes.parquet"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def LPR3_DIAGNOSER_OUT(self) -> Path:
        return self.DATA_DIR / "diagnoser" / "diagnoser.parquet"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def LPR3_KONTAKTER_OUT(self) -> Path:
        return self.DATA_DIR / "kontakter" / "kontakter.parquet"

    @staticmethod
    def get_mapping_path(filename: str) -> Path:
        """Get the path to a mapping file."""
        with pkg_resources.as_file(
            pkg_resources.files("cdef_cohort_generation").joinpath("..", "..", "mappings", filename)
        ) as path:
            return Path(path)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def ISCED_MAPPING_FILE(self) -> Path:
        return self.get_mapping_path("isced.json")


# Create a global instance of the Settings
settings = Settings()
