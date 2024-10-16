from cdef_cohort_builder.settings import settings

# Use settings directly
BASE_DIR = settings.BASE_DIR
DATA_DIR = settings.DATA_DIR
POPULATION_FILE = settings.POPULATION_FILE
STATIC_COHORT = settings.STATIC_COHORT
COHORT_FILE = settings.COHORT_FILE
HASH_FILE_PATH = settings.HASH_FILE_PATH
ISCED_FILE = settings.ISCED_FILE
ICD_FILE = settings.ICD_FILE
ISCED_MAPPING_FILE = settings.ISCED_MAPPING_FILE
PARQUETS = settings.PARQUETS
BIRTH_INCLUSION_START_YEAR = settings.BIRTH_INCLUSION_START_YEAR
BIRTH_INCLUSION_END_YEAR = settings.BIRTH_INCLUSION_END_YEAR

# Registers
REGISTER_DIR = settings.REGISTER_BASE_DIR

# Demographic data
BEF_FILES = settings.BEF_FILES
UDDF_FILES = settings.UDDF_FILES

# Health data
LPR_ADM_FILES = settings.LPR_ADM_FILES
LPR_DIAG_FILES = settings.LPR_DIAG_FILES
LPR_BES_FILES = settings.LPR_BES_FILES
LPR3_DIAGNOSER_FILES = settings.LPR3_DIAGNOSER_FILES
LPR3_KONTAKTER_FILES = settings.LPR3_KONTAKTER_FILES

# Socioeconomic data
AKM_FILES = settings.AKM_FILES
IDAN_FILES = settings.IDAN_FILES
IND_FILES = settings.IND_FILES

# Output files
BEF_OUT = settings.BEF_OUT
UDDF_OUT = settings.UDDF_OUT
LPR_ADM_OUT = settings.LPR_ADM_OUT
AKM_OUT = settings.AKM_OUT
IDAN_OUT = settings.IDAN_OUT
IND_OUT = settings.IND_OUT
LPR_DIAG_OUT = settings.LPR_DIAG_OUT
LPR_BES_OUT = settings.LPR_BES_OUT
LPR3_DIAGNOSER_OUT = settings.LPR3_DIAGNOSER_OUT
LPR3_KONTAKTER_OUT = settings.LPR3_KONTAKTER_OUT

EVENT_DEFINITIONS = settings.EVENT_DEFINITIONS