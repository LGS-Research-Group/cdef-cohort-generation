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


def process_akm(columns_to_keep: list[str] | None = None, **kwargs: KwargsType) -> None:
    default_columns = ["PNR", "SOCIO13", "SENR", "year"]
    columns = columns_to_keep if columns_to_keep is not None else default_columns

    process_register_data(
        input_files=AKM_FILES,
        output_file=AKM_OUT,
        population_file=POPULATION_FILE,
        schema=AKM_SCHEMA,
        columns_to_keep=columns,
        join_parents_only=True,
        **kwargs,
    )


if __name__ == "__main__":
    process_akm()
