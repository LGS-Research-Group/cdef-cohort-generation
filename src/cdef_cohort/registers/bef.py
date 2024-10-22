import polars as pl
from cdef_cohort.logging_config import logger
from cdef_cohort.mapping_utils import apply_mapping
from cdef_cohort.registers.generic import process_register_data
from cdef_cohort.utils.config import (
    BEF_FILES,
    BEF_OUT,
    POPULATION_FILE,
)
from cdef_cohort.utils.logging_decorator import log_processing
from cdef_cohort.utils.types import KwargsType

BEF_SCHEMA = {
    "AEGTE_ID": pl.Utf8,
    "ALDER": pl.Int8,
    "ANTBOERNF": pl.Int8,
    "ANTBOERNH": pl.Int8,
    "ANTPERSF": pl.Int8,
    "ANTPERSH": pl.Int8,
    "BOP_VFRA": pl.Date,
    "CIVST": pl.Categorical,
    "CPRTJEK": pl.Int8,
    "CPRTYPE": pl.Int8,
    "E_FAELLE_ID": pl.Utf8,
    "FAMILIE_ID": pl.Utf8,
    "FAMILIE_TYPE": pl.UInt8,
    "FAR_ID": pl.Utf8,
    "FM_MARK": pl.Categorical,
    "FOED_DAG": pl.Date,
    "HUSTYPE": pl.Categorical,
    "IE_TYPE": pl.Utf8,
    "KOEN": pl.Utf8,
    "KOM": pl.Int8,
    "MOR_ID": pl.Utf8,
    "OPR_LAND": pl.Utf8,
    "PLADS": pl.Categorical,
    "PNR": pl.Utf8,
    "REG": pl.Categorical,
    "STATSB": pl.Categorical,
    "VERSION": pl.Utf8,
}

BEF_DEFAULTS = {
    "population_file": POPULATION_FILE,
    "columns_to_keep": [
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
    ],
    "date_columns": ["FOED_DAG", "BOP_VFRA"],
    "join_parents_only": False,
    "longitudinal": True,
}

logger.debug(f"BEF_SCHEMA: {BEF_SCHEMA}")
logger.debug(f"BEF_DEFAULTS: {BEF_DEFAULTS}")


def preprocess_bef(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns(
        [
            pl.col("FM_MARK").cast(pl.Utf8).pipe(apply_mapping, "fm_mark").cast(pl.Categorical),
            pl.col("CIVST").cast(pl.Utf8).pipe(apply_mapping, "civst").cast(pl.Categorical),
            pl.col("HUSTYPE").cast(pl.Utf8).pipe(apply_mapping, "hustype").cast(pl.Categorical),
            pl.col("PLADS").cast(pl.Utf8).pipe(apply_mapping, "plads").cast(pl.Categorical),
            pl.col("REG").cast(pl.Utf8).pipe(apply_mapping, "reg").cast(pl.Categorical),
            pl.col("STATSB").cast(pl.Utf8).pipe(apply_mapping, "statsb").cast(pl.Categorical),
        ]
    )


@log_processing
def process_bef(**kwargs: KwargsType) -> None:
    process_register_data(
        input_files=BEF_FILES,
        output_file=BEF_OUT,
        schema=BEF_SCHEMA,
        defaults=BEF_DEFAULTS,
        register_name="BEF",
        preprocess_func=preprocess_bef,
        **kwargs,
    )


if __name__ == "__main__":
    logger.debug("Running process_bef as main")
    process_bef()
    logger.debug("Finished running process_bef as main")
