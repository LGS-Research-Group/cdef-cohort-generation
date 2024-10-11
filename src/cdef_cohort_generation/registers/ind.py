import polars as pl

from cdef_cohort_generation.utils import (
    IND_FILES,
    IND_OUT,
    POPULATION_FILE,
    KwargsType,
    process_register_data,
)

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


def process_ind(columns_to_keep: list[str] | None = None, **kwargs: KwargsType) -> None:
    default_columns = ["PNR", "BESKST13", "LOENMV_13", "PERINDKIALT_13", "PRE_SOCIO", "year"]
    # Use default_columns if columns_to_keep is None
    columns = columns_to_keep if columns_to_keep is not None else default_columns
    process_register_data(
        input_files=IND_FILES,
        output_file=IND_OUT,
        population_file=POPULATION_FILE,
        schema=IND_SCHEMA,
        columns_to_keep=columns,
        join_parents_only=True,
        longitudinal=True,
        **kwargs,
    )


if __name__ == "__main__":
    process_ind()
