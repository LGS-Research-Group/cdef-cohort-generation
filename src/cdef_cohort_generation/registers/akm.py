import polars as pl

from cdef_cohort_generation.utils import (
    AKM_FILES,
    AKM_OUT,
    POPULATION_FILE,
    KwargsType,
    process_register_data,
)

AKM_SCHEMA = {
    "PNR": pl.Utf8,
    "SOCIO": pl.Int8,
    "SOCIO02": pl.Int8,
    "SOCIO13": pl.Int8,  # <- Only one we are interested in
    "CPRTJEK": pl.Utf8,
    "CPRTYPE": pl.Utf8,
    "VERSION": pl.Utf8,
    "SENR": pl.Utf8,  # Dont know the structure of this
}

AKM_DEFAULTS = {
    "population_file": POPULATION_FILE,
    "columns_to_keep": ["PNR", "SOCIO13", "SENR", "year"],
    "join_parents_only": True,
    "longitudinal": False,
}


def process_akm(**kwargs: KwargsType) -> None:
    """Process AKM data, join with population data, and save the result."""
    process_register_data(
        input_files=AKM_FILES,
        output_file=AKM_OUT,
        schema=AKM_SCHEMA,
        defaults=AKM_DEFAULTS,
        **kwargs,
    )


if __name__ == "__main__":
    process_akm()
