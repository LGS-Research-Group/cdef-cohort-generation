from pathlib import Path

import polars as pl
import polars.selectors as cs

from cdef_cohort_builder.logging_config import logger
from cdef_cohort_builder.registers import akm, bef, idan, ind, uddf
from cdef_cohort_builder.utils.config import (
    AKM_OUT,
    BEF_OUT,
    IDAN_OUT,
    IND_OUT,
    STATIC_COHORT,
    UDDF_OUT,
)
from cdef_cohort_builder.utils.hash_utils import process_with_hash_check


def process_and_partition_longitudinal_data(output_dir: Path) -> None:
    """Process, combine, and partition longitudinal data from various registers."""

    logger.info("Processing longitudinal data")
    common_params = {
        "population_file": STATIC_COHORT,
        "longitudinal": True,
    }

    logger.debug("Processing individual registers")
    process_with_hash_check(bef.process_bef, **common_params)
    process_with_hash_check(akm.process_akm, **common_params)
    process_with_hash_check(ind.process_ind, **common_params)
    process_with_hash_check(idan.process_idan, **common_params)
    process_with_hash_check(uddf.process_uddf, **common_params)

    longitudinal_registers = [
        (BEF_OUT, "BEF"),
        (AKM_OUT, "AKM"),
        (IND_OUT, "IND"),
        (IDAN_OUT, "IDAN"),
        (UDDF_OUT, "UDDF"),
    ]
    longitudinal_data = []
    all_columns = set()

    for register_file, register_name in longitudinal_registers:
        logger.debug(f"Reading data from {register_file}")
        register_data = pl.scan_parquet(register_file)
        logger.debug(f"Schema for {register_name}: {register_data.collect_schema()}")
        all_columns.update(register_data.collect_schema().names())
        longitudinal_data.append(register_data)

    logger.debug(f"All columns across registers: {all_columns}")

    logger.info("Concatenating longitudinal data from all registers")
    combined_data = pl.concat(longitudinal_data, how="diagonal")

    logger.info("Transforming and partitioning combined data")
    logger.debug(f"Columns in combined_data: {combined_data.collect_schema().names()}")

    # Separate child, mother, and father data
    child_cols = cs.by_name(["PNR", "year", "month"]) | (cs.all() - cs.starts_with("FAR_", "MOR_"))
    mother_cols = cs.starts_with("MOR_")
    father_cols = cs.starts_with("FAR_")

    logger.debug(f"Child columns: {cs.expand_selector(combined_data, child_cols)}")
    logger.debug(f"Mother columns: {cs.expand_selector(combined_data, mother_cols)}")
    logger.debug(f"Father columns: {cs.expand_selector(combined_data, father_cols)}")

    # Process child data
    child_data = combined_data.select(child_cols)
    child_data = rename_duplicates(child_data)
    logger.debug(f"Child data schema after renaming: {child_data.collect_schema()}")
    logger.debug(f"Columns in child_data before writing: {child_data.collect_schema().names()}")

    output_dir.mkdir(parents=True, exist_ok=True)
    child_data.collect().write_parquet(output_dir / "combined_longitudinal.parquet")

    # Process mother data if exists
    if cs.expand_selector(combined_data, mother_cols):
        mother_data = combined_data.select(
            cs.by_name(["PNR", "year", "month", "MOR_ID"]) | mother_cols
        )
        mother_data = mother_data.rename({"PNR": "CHILD_PNR", "MOR_ID": "MOTHER_PNR"})
        logger.debug(f"Mother data schema: {mother_data.collect_schema()}")
        (output_dir / "parent_data").mkdir(parents=True, exist_ok=True)
        mother_data.collect().write_parquet(
            output_dir / "parent_data" / "mother_longitudinal.parquet"
        )

    # Process father data if exists
    if cs.expand_selector(combined_data, father_cols):
        father_data = combined_data.select(
            cs.by_name(["PNR", "year", "month", "FAR_ID"]) | father_cols
        )
        father_data = father_data.rename({"PNR": "CHILD_PNR", "FAR_ID": "FATHER_PNR"})
        logger.debug(f"Father data schema: {father_data.collect_schema()}")
        (output_dir / "parent_data").mkdir(parents=True, exist_ok=True)
        father_data.collect().write_parquet(
            output_dir / "parent_data" / "father_longitudinal.parquet"
        )

    logger.info(f"Transformed and partitioned combined data saved to {output_dir}")
    return combined_data


def rename_duplicates(df):
    columns = df.collect_schema().names()
    new_names = []
    seen = set()
    for col in columns:
        new_name = col
        i = 1
        while new_name in seen:
            new_name = f"{col}_{i}"
            i += 1
        new_names.append(new_name)
        seen.add(new_name)
    return df.rename(dict(zip(columns, new_names, strict=False)))


if __name__ == "__main__":
    # Example usage
    output_dir = Path("path/to/your/output/directory")

    process_and_partition_longitudinal_data(output_dir)
