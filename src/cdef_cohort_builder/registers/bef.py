import polars as pl

from cdef_cohort_builder.logging_config import logger
from cdef_cohort_builder.registers.generic import process_register_data
from cdef_cohort_builder.utils.config import (
    BEF_FILES,
    BEF_OUT,
    POPULATION_FILE,
)
from cdef_cohort_builder.utils.logging_decorator import log_processing
from cdef_cohort_builder.utils.types import KwargsType

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


@log_processing
def process_bef(**kwargs: KwargsType) -> None:
    process_register_data(
        input_files=BEF_FILES,
        output_file=BEF_OUT,
        schema=BEF_SCHEMA,
        defaults=BEF_DEFAULTS,
        register_name="BEF",
        **kwargs,
    )


if __name__ == "__main__":
    logger.debug("Running process_bef as main")
    process_bef()
    logger.debug("Finished running process_bef as main")
