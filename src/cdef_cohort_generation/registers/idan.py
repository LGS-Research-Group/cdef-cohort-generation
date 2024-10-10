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

JOBKAT_map = {
    1: "Fuld tid (30 timer eller mere pr. uge)",
    2: "Deltid (15-29 timer)",
    3: "Bijob (under 15 timer)",
    5: "Fuld tid (Over 8 uger med delvis ledighed i løbet af året)",
    6: "Deltid (Over 8 uger med delvis ledighed i løbet af året)",
    7: "Bijob (Over 8 uger med delvis ledighed i løbet af året)",
    9: "Uoplyst",
}

TILKNYT_map = {
    1: "Heltid, kontinuert, længere end et år",
    2: "Deltid (>= 30 timer), kontinuert, længere end et år",
    3: "Deltid (>= 20-29 timer), kontinuert, længere end et år",
    4: "Deltid (>= 10-19 timer), kontinuert, længere end et år",
    5: "Deltid (< 10 timer), kontinuert, længere end et år",
    11: "Heltid, serielt, længere end et år",
    12: "Deltid (>= 30 timer), serielt, længere end et år",
    13: "Deltid (>= 20-29 timer), serielt, længere end et år",
    14: "Deltid (>= 10-19 timer), serielt, længere end et år",
    15: "Deltid (< 10 timer), serielt, længere end et år",
    21: "Heltid, kontinuert, kortere end et år",
    22: "Deltid (>= 30 timer), kontinuert, kortere end et år",
    23: "Deltid (>= 20-29 timer), kontinuert, kortere end et år",
    24: "Deltid (>= 10-19 timer), kontinuert, kortere end et år",
    25: "Deltid (< 10 timer), kontinuert, kortere end et år",
    31: "Heltid, serielt, kortere end et år",
    32: "Deltid (>= 30 timer), serielt, kortere end et år",
    33: "Deltid (>= 20-29 timer), serielt, kortere end et år",
    34: "Deltid (>= 10-19 timer), serielt, kortere end et år",
    35: "Deltid (< 10 timer), serielt, kortere end et år",
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
