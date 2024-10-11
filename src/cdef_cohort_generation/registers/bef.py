import polars as pl

from cdef_cohort_generation.utils import (
    BEF_FILES,
    BEF_OUT,
    POPULATION_FILE,
    KwargsType,
    process_register_data,
)

BEF_SCHEMA = {
    "AEGTE_ID": pl.Utf8,
    "ALDER": pl.Int8,
    "ANTBOERNF": pl.Int8,
    "ANTBOERNH": pl.Int8,
    "ANTPERSF": pl.Int8,
    "ANTPERSH": pl.Int8,
    "BOP_VFRA": pl.Date,
    "CIVST": pl.Utf8,
    "CPRTJEK": pl.Int8,
    "CPRTYPE": pl.Int8,
    "E_FAELLE_ID": pl.Utf8,
    "FAMILIE_ID": pl.Utf8,
    "FAMILIE_TYPE": pl.UInt8,  # ved ikke hvordan den her varible ser ud
    "FAR_ID": pl.Utf8,
    "FM_MARK": pl.Int8,
    "FOED_DAG": pl.Date,
    "HUSTYPE": pl.Int8,
    "IE_TYPE": pl.Utf8,  # ved ikke hvordan den her varible ser ud
    "KOEN": pl.Utf8,  # ved ikke hvordan den her varible ser ud
    "KOM": pl.Int8,  # 2-3 cifret kode
    "MOR_ID": pl.Utf8,
    "OPR_LAND": pl.Utf8,  # ved ikke hvordan den har varitable ser ud
    "PLADS": pl.Int8,
    "PNR": pl.Utf8,
    "REG": pl.Int8,
    "STATSB": pl.Int8,
    "VERSION": pl.Utf8,
}


def process_bef(columns_to_keep: list[str] | None = None, **kwargs: KwargsType) -> None:
    default_columns = [
        "AEGTE_ID",
        "ALDER",
        "ANTBOERNF",
        "ANTBOERNH",
        "ANTPERSF",
        "ANTPERSH",
        "BOP_VFRA",
        "CIVST",
        "E_FAELLE_ID",
        "FAMILIE_ID",
        "FAMILIE_TYPE",
        "FAR_ID",
        "FM_MARK",
        "FOED_DAG",
        "HUSTYPE",
        "IE_TYPE",
        "KOEN",
        "KOM",
        "MOR_ID",
        "OPR_LAND",
        "PLADS",
        "PNR",
        "REG",
        "STATSB",
        "year",
        "month",
    ]
    # Use default_columns if columns_to_keep is None
    columns = columns_to_keep if columns_to_keep is not None else default_columns
    process_register_data(
        input_files=BEF_FILES,
        output_file=BEF_OUT,
        population_file=POPULATION_FILE,
        schema=BEF_SCHEMA,
        date_columns=["FOED_DAG", "BOP_VFRA"],
        columns_to_keep=columns,
        join_parents_only=False,
        longitudinal=True,
        **kwargs,
    )


if __name__ == "__main__":
    process_bef()
