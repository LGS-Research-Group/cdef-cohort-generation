import polars as pl

from cdef_cohort_generation.utils import (
    IDAN_FILES,
    IDAN_OUT,
    POPULATION_FILE,
    KwargsType,
    process_register_data,
)

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


IDAN_DEFAULTS = {
    "population_file": POPULATION_FILE,
    "columns_to_keep": [
        "PNR",
        "ARBGNR",
        "ARBNR",
        "CVRNR",
        "JOBKAT",
        "JOBLON",
        "LBNR",
        "STILL",
        "TILKNYT",
        "year",
    ],
    "join_parents_only": True,
    "longitudinal": True,
}


def process_idan(**kwargs: KwargsType) -> None:
    process_register_data(
        input_files=IDAN_FILES,
        output_file=IDAN_OUT,
        schema=IDAN_SCHEMA,
        defaults=IDAN_DEFAULTS,
        **kwargs,
    )


if __name__ == "__main__":
    process_idan()
