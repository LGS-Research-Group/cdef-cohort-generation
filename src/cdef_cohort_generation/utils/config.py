from pathlib import Path

import polars as pl

# Constants
BASE_DIR = Path("/Users/tobiaskragholm/dev/TEST_RUN")
DATA_DIR = BASE_DIR / "data"
POPULATION_FILE = DATA_DIR / "population.parquet"
STATIC_COHORT = DATA_DIR / "static_cohort.parquet"

COHORT_FILE = DATA_DIR / "cohort.parquet"
ISCED_FILE = DATA_DIR / "isced.parquet"
ICD_FILE = DATA_DIR / "icd10dict.csv"
RDAT_FILE = Path("path/to/your/uddf.rda")
ISCED_TSV_FILE = Path("/Users/tobiaskragholm/dev/cdef-cohort-generation/data/ISCED.tsv")
PARQUETS = "*.parquet"
BIRTH_INCLUSION_START_YEAR = 1995
BIRTH_INCLUSION_END_YEAR = 2020

# Registers
REGISTER_DIR = Path("/Users/tobiaskragholm/dev/TEST_RUN/registers")

# Demographic data
BEF_FILES = REGISTER_DIR / "bef" / PARQUETS
UDDF_FILES = REGISTER_DIR / "uddf" / PARQUETS

# Health data
LPR_ADM_FILES = REGISTER_DIR / "lpr_adm" / PARQUETS
LPR_DIAG_FILES = REGISTER_DIR / "lpr_diag" / PARQUETS
LPR_BES_FILES = REGISTER_DIR / "lpr_bes" / PARQUETS
LPR3_DIAGNOSER_FILES = REGISTER_DIR / "lpr3_diagnoser" / PARQUETS
LPR3_KONTAKTER_FILES = REGISTER_DIR / "lpr3_kontakter" / PARQUETS

# Socioeconomic data
AKM_FILES = REGISTER_DIR / "akm" / PARQUETS
IDAN_FILES = REGISTER_DIR / "idan" / PARQUETS
IND_FILES = REGISTER_DIR / "ind" / PARQUETS

# Output files
BEF_OUT = DATA_DIR / "bef" / "bef.parquet"
UDDF_OUT = DATA_DIR / "uddf" / "uddf.parquet"
LPR_ADM_OUT = DATA_DIR / "lpr_adm" / "lpr_adm.parquet"
AKM_OUT = DATA_DIR / "akm" / "akm.parquet"
IDAN_OUT = DATA_DIR / "idan" / "idan.parquet"
IND_OUT = DATA_DIR / "ind" / "ind.parquet"
LPR_DIAG_OUT = DATA_DIR / "lpr_diag" / "lpr_diag.parquet"
LPR_BES_OUT = DATA_DIR / "lpr_bes" / "lpr_bes.parquet"
LPR3_DIAGNOSER_OUT = DATA_DIR / "diagnoser" / "diagnoser.parquet"
LPR3_KONTAKTER_OUT = DATA_DIR / "kontakter" / "kontakter.parquet"

EVENT_DEFINITIONS = {
    "father_education_change": (pl.col("FAR_EDU_LVL").shift() != pl.col("FAR_EDU_LVL")),
    "mother_education_change": (pl.col("MOR_EDU_LVL").shift() != pl.col("MOR_EDU_LVL")),
    "father_income_change": (pl.col("FAR_PERINDKIALT_13").cast(pl.Float64).diff() != 0),
    "mother_income_change": (pl.col("MOR_PERINDKIALT_13").cast(pl.Float64).diff() != 0),
    "municipality_change": (pl.col("KOM").shift() != pl.col("KOM")),
}
