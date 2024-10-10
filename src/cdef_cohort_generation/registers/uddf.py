import polars as pl

from cdef_cohort_generation.config import POPULATION_FILE, UDDF_FILES, UDDF_OUT
from cdef_cohort_generation.utils import process_register_data

UDDF_SCHEMA = {
    "PNR": pl.Utf8,
    "CPRTJEK": pl.Utf8,
    "CPRTYPE": pl.Utf8,
    "HFAUDD": pl.Utf8,
    "HF_KILDE": pl.Utf8,
    "HF_VFRA": pl.Utf8,
    "HF_VTIL": pl.Utf8,
    "INSTNR": pl.Int8,
    "VERSION": pl.Utf8,
}


def process_uddf() -> None:
    process_register_data(
        input_files=UDDF_FILES,
        output_file=UDDF_OUT,
        population_file=POPULATION_FILE,
        schema=UDDF_SCHEMA,
        date_columns=["HF_VFRA", "HF_VTIL"],
        columns_to_keep=[
            "PNR",
            "HFAUDD",
            "HF_KILDE",
            "HF_VFRA",
            "INSTNR",
        ],
    )


if __name__ == "__main__":
    process_uddf()
