from pathlib import Path

import polars as pl

# Constants
BASE_DIR = Path("E:/workdata/708245/CDEF/Projekter/Family")
DATA_DIR = BASE_DIR / "data"
POPULATION_FILE = DATA_DIR / "population.parquet"
COHORT_FILE = DATA_DIR / "cohort.parquet"
ISCED_FILE = DATA_DIR / "isced.parquet"
ICD_FILE = DATA_DIR / "icd10dict.csv"
RDAT_FILE = Path("path/to/your/uddf.rda")
PARQUETS = "*.parquet"
PARQUET_SUFFIX = ".parquet"
BIRTH_INCLUSION_START_YEAR = 1995
BIRTH_INCLUSION_END_YEAR = 2020

# Registers
REGISTER_DIR = Path("E:/workdata/708245/data/register")

# Demographic data
BEF_FILES = REGISTER_DIR / "bef" / PARQUETS
UDDF_FILES = REGISTER_DIR / "uddf" / PARQUETS

# Health data
LPR_ADM_FILES = REGISTER_DIR / "lpr_adm" / PARQUETS
LPR_DIAG_FILES = REGISTER_DIR / "lpr_diag" / PARQUETS
LPR_BES_FILES = REGISTER_DIR / "lpr_bes" / PARQUETS
LPR3_DIAGNOSER_FILES = REGISTER_DIR / "diagnoser" / PARQUETS
LPR3_KONTAKTER_FILES = REGISTER_DIR / "kontakter" / PARQUETS

# Socioeconomic data
AKM_FILES = REGISTER_DIR / "akm" / PARQUETS
IDAN_FILES = REGISTER_DIR / "idan" / PARQUETS
IND_FILES = REGISTER_DIR / "ind" / PARQUETS

# Output files
BEF_OUT = DATA_DIR / "bef/bef" / PARQUET_SUFFIX
UDDF_OUT = DATA_DIR / "uddf/uddf" / PARQUET_SUFFIX
LPR_ADM_OUT = DATA_DIR / "lpr_adm/lpr_adm" / PARQUET_SUFFIX
AKM_OUT = DATA_DIR / "akm/akm" / PARQUET_SUFFIX
IDAN_OUT = DATA_DIR / "idan/idan" / PARQUET_SUFFIX
IND_OUT = DATA_DIR / "ind/ind" / PARQUET_SUFFIX
LPR_DIAG_OUT = DATA_DIR / "lpr_diag/lpr_diag" / PARQUET_SUFFIX
LPR_BES_OUT = DATA_DIR / "lpr_bes/lpr_bes" / PARQUET_SUFFIX
LPR3_DIAGNOSER_OUT = DATA_DIR / "diagnoser/diagnoser" / PARQUET_SUFFIX
LPR3_KONTAKTER_OUT = DATA_DIR / "kontakter/kontakter" / PARQUET_SUFFIX


EVENT_DEFINITIONS = {
    "education_change": (pl.col("EDU_LVL").diff() != 0),
    "income_bracket_change": (pl.col("income_bracket").diff() != 0),
    "municipality_change": (pl.col("KOM").diff() != 0),
    # Add more event definitions as needed
}
