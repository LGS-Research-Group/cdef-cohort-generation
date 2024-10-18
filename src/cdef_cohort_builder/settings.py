import importlib.resources as pkg_resources
import sys
from pathlib import Path
from typing import Any, Literal

import polars as pl
from pydantic import ValidationError, computed_field, validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# from cdef_cohort_builder.logging_config import LogLevel, logger

LogLevel = Literal["debug", "info", "warning", "error", "critical"]


class Settings(BaseSettings):
    # Basic paths
    BASE_DIR: Path = Path("/Users/tobiaskragholm/dev/TEST_RUN")
    REGISTER_BASE_DIR: Path = Path("/Users/tobiaskragholm/dev/TEST_RUN/registers")

    # Constants
    PARQUETS: str = "*.parquet"
    BIRTH_INCLUSION_START_YEAR: int = 1995
    BIRTH_INCLUSION_END_YEAR: int = 2020
    LOG_LEVEL: LogLevel = "info"

    EVENT_DEFINITIONS: dict[str, Any] = {
        # Existing events
        "father_education_change": (pl.col("FAR_EDU_LVL").shift() != pl.col("FAR_EDU_LVL")),
        "mother_education_change": (pl.col("MOR_EDU_LVL").shift() != pl.col("MOR_EDU_LVL")),
        "father_income_change": (pl.col("FAR_PERINDKIALT_13").cast(pl.Float64).diff() != 0),
        "mother_income_change": (pl.col("MOR_PERINDKIALT_13").cast(pl.Float64).diff() != 0),
        "municipality_change": (pl.col("KOM").shift() != pl.col("KOM")),
        # New events
        "family_composition_change": (pl.col("FAMILIE_TYPE").shift() != pl.col("FAMILIE_TYPE")),
        "marital_status_change": (pl.col("CIVST").shift() != pl.col("CIVST")),
        "significant_income_increase_father": (
            pl.col("FAR_PERINDKIALT_13").cast(pl.Float64).pct_change() > 0.10
        ),
        "significant_income_increase_mother": (
            pl.col("MOR_PERINDKIALT_13").cast(pl.Float64).pct_change() > 0.10
        ),
        "significant_income_decrease_father": (
            pl.col("FAR_PERINDKIALT_13").cast(pl.Float64).pct_change() < -0.10
        ),
        "significant_income_decrease_mother": (
            pl.col("MOR_PERINDKIALT_13").cast(pl.Float64).pct_change() < -0.10
        ),
        "father_employment_status_change": (
            pl.col("FAR_BESKST13").shift() != pl.col("FAR_BESKST13")
        ),
        "mother_employment_status_change": (
            pl.col("MOR_BESKST13").shift() != pl.col("MOR_BESKST13")
        ),
        "father_job_change": (pl.col("FAR_STILL").shift() != pl.col("FAR_STILL")),
        "mother_job_change": (pl.col("MOR_STILL").shift() != pl.col("MOR_STILL")),
        "household_size_increase": (pl.col("ANTPERSH").diff() > 0),
        "household_size_decrease": (pl.col("ANTPERSH").diff() < 0),
        "number_of_children_increase": (pl.col("ANTBOERNH").diff() > 0),
        "number_of_children_decrease": (pl.col("ANTBOERNH").diff() < 0),
        "father_socioeconomic_status_change": (
            pl.col("FAR_SOCIO13").shift() != pl.col("FAR_SOCIO13")
        ),
        "mother_socioeconomic_status_change": (
            pl.col("MOR_SOCIO13").shift() != pl.col("MOR_SOCIO13")
        ),
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
    def HASH_FILE_PATH(self) -> Path:
        return self.DATA_DIR / "hash_file.json"

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
            pkg_resources.files("cdef_cohort_builder").joinpath("mappings", filename)
        ) as path:
            return Path(path)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def ISCED_MAPPING_FILE(self) -> Path:
        return self.get_mapping_path("isced.json")

    @validator("BASE_DIR", "REGISTER_BASE_DIR", pre=True)
    def validate_directory(cls, v: Any) -> Path:
        path = Path(v)
        if not path.exists():
            raise ValueError(f"Directory does not exist: {path}")
        return path

    @validator("LOG_LEVEL")
    def validate_log_level(cls, v: str) -> str:
        if v.lower() not in ["debug", "info", "warning", "error", "critical"]:
            raise ValueError(f"Invalid log level: {v}")
        return v.lower()

    @validator(
        "BEF_FILES",
        "UDDF_FILES",
        "LPR_ADM_FILES",
        "LPR_DIAG_FILES",
        "LPR_BES_FILES",
        "LPR3_DIAGNOSER_FILES",
        "LPR3_KONTAKTER_FILES",
        "AKM_FILES",
        "IDAN_FILES",
        "IND_FILES",
        pre=True,
    )
    def validate_files_exist(cls, v: Any) -> Path:
        path = Path(v)
        if not path.parent.exists():
            raise ValueError(f"Parent directory does not exist for: {path}")
        if not list(path.parent.glob(path.name)):
            raise ValueError(f"No files found matching pattern: {path}")
        return path


# Create a global instance of the Settings with error handling
try:
    settings = Settings()
except ValidationError as e:
    print("Error in configuration:")
    for error in e.errors():
        field = ".".join(str(loc) for loc in error["loc"]) if error["loc"] else "Unknown field"
        message = error["msg"]
        print(f"- {field}: {message}")
    print("\nPlease check your configuration settings and ensure all required directories exist.")
    print("Configuration is typically set in the .env file or environment variables.")
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred while loading configuration: {str(e)}")
    sys.exit(1)


# Function to check if settings loaded successfully
def check_settings() -> bool:
    return settings is not None
