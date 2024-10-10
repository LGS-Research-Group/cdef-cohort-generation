import polars as pl

from cdef_cohort_generation.config import IDAN_FILES, IDAN_OUT, POPULATION_FILE
from cdef_cohort_generation.utils import process_register_data

IDAN_SCHEMA = {
    "ARBGNR": pl.Utf8,  # Arbejdsgivernummer
    "ARBNR": pl.Utf8,  # Arbejdsstedsnummer
    "CPRTJEK": pl.Utf8,
    "CPRTYPE": pl.Utf8,
    "CVRNR": pl.Utf8,
    "JOBKAT": pl.Int8,  # See JOBKAT_map
    "JOBLON": pl.Float64,  # salary
    "LBNR": pl.Utf8,
    "PNR": pl.Utf8,
    "STILL": pl.Utf8,  # a variation of job title
    "TILKNYT": pl.Int8,  # See TILKNYT_map
}


def process_idan() -> None:
    process_register_data(
        input_files=IDAN_FILES,
        output_file=IDAN_OUT,
        population_file=POPULATION_FILE,
        schema=IDAN_SCHEMA,
        columns_to_keep=[
            "PNR",
            "ARBGNR",
            "ARBNR",
            "CVRNR",
            "JOBKAT",
            "JOBLON",
            "LBNR",
            "STILL",
            "TILKNYT",
        ],
    )


if __name__ == "__main__":
    process_idan()