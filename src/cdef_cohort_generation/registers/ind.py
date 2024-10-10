import polars as pl

from cdef_cohort_generation.config import IND_FILES, IND_OUT, POPULATION_FILE
from cdef_cohort_generation.utils import process_register_data

IND_SCHEMA = {
    "BESKST13": pl.Int8,  # Kode for personens væsentligste indkomstkilde
    "CPRTJEK": pl.Utf8,
    "CPRTYPE": pl.Utf8,
    "LOENMV_13": pl.Float64,  # Lønindkomst
    "PERINDKIALT_13": pl.Float64,  # Personlig indkomst
    "PNR": pl.Utf8,
    "PRE_SOCIO": pl.Int8,  # See mapping
    "VERSION": pl.Utf8,
}


def process_ind() -> None:
    process_register_data(
        input_files=IND_FILES,
        output_file=IND_OUT,
        population_file=POPULATION_FILE,
        schema=IND_SCHEMA,
        columns_to_keep=[
            "PNR",
            "BESKST13",
            "LOENMV_13",
            "PERINDKIALT_13",
            "PRE_SOCIO",
        ],
    )


if __name__ == "__main__":
    process_ind()
