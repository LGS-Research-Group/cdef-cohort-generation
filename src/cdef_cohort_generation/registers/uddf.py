import polars as pl

from cdef_cohort_generation.utils import (
    POPULATION_FILE,
    UDDF_FILES,
    UDDF_OUT,
    KwargsType,
    process_register_data,
)

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


def process_uddf(columns_to_keep: list[str] | None = None, **kwargs: KwargsType) -> None:
    default_columns = [
        "PNR",
        "HFAUDD",
        "HF_KILDE",
        "HF_VFRA",
        "INSTNR",
    ]
    # Use default_columns if columns_to_keep is None
    columns = columns_to_keep if columns_to_keep is not None else default_columns

    process_register_data(
        input_files=UDDF_FILES,
        output_file=UDDF_OUT,
        population_file=POPULATION_FILE,
        schema=UDDF_SCHEMA,
        date_columns=["HF_VFRA", "HF_VTIL"],
        columns_to_keep=columns,
        join_parents_only=True,
        register_name="UDDF",
        **kwargs,
    )


if __name__ == "__main__":
    process_uddf()
