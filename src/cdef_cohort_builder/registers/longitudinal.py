from collections.abc import Callable
from pathlib import Path
from typing import Any

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


def log_non_null_counts(df: pl.LazyFrame, name: str) -> None:
    counts = df.select(pl.all().is_not_null().sum()).collect().to_dict()
    logger.debug(f"Non-null counts for {name}:")
    for col, count in counts.items():
        logger.debug(f"  {col}: {count[0]}")


def process_and_partition_longitudinal_data(
    output_dir: Path,
) -> tuple[pl.LazyFrame, pl.LazyFrame | None, pl.LazyFrame | None]:
    """
    Process, combine, and partition longitudinal data from various registers.

    This function processes data from multiple registers (BEF, AKM, IND, IDAN, UDDF),
    combines them, and partitions the result into child, mother, and father data.

    Args:
        output_dir (Path): The directory where the processed data will be saved.

    Returns:
        tuple[pl.LazyFrame, pl.LazyFrame | None, pl.LazyFrame | None]: A tuple containing:
            - child_data (pl.LazyFrame): Processed child data.
            - mother_data (pl.LazyFrame | None): Processed mother data, if available.
            - father_data (pl.LazyFrame | None): Processed father data, if available.

    Raises:
        ValueError: If no valid longitudinal data is found.

    Note:
        This function logs various debug and info messages throughout its execution.
    """
    # Check if all parquet files exist
    child_parquet = output_dir / "long" / "child.parquet"
    mother_parquet = output_dir / "long" / "mother.parquet"
    father_parquet = output_dir / "long" / "father.parquet"

    if child_parquet.exists() and mother_parquet.exists() and father_parquet.exists():
        logger.info("Existing parquet files found. Reading data from files.")
        child_data = pl.scan_parquet(child_parquet)
        mother_data = pl.scan_parquet(mother_parquet)
        father_data = pl.scan_parquet(father_parquet)
        return child_data, mother_data, father_data

    logger.info("Processing longitudinal data")
    common_params = {
        "population_file": STATIC_COHORT,
        "longitudinal": True,
    }

    logger.debug("Processing individual registers")
    registers_to_process: list[tuple[Callable[..., Any], str]] = [
        (bef.process_bef, "BEF"),
        (akm.process_akm, "AKM"),
        (ind.process_ind, "IND"),
        (idan.process_idan, "IDAN"),
        (uddf.process_uddf, "UDDF"),
    ]

    for process_func, register_name in registers_to_process:
        try:
            process_with_hash_check(process_func, **common_params)
        except Exception as e:
            logger.warning(f"Error processing {register_name}: {str(e)}. Skipping this register.")

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
        try:
            logger.debug(f"Reading data from {register_file}")
            register_data = pl.scan_parquet(register_file)
            logger.debug(f"Schema for {register_name}: {register_data.collect_schema()}")
            all_columns.update(register_data.collect_schema().names())
            longitudinal_data.append(register_data)
        except Exception as e:
            logger.warning(f"Error reading {register_name}: {str(e)}. Skipping this register.")

    logger.debug(f"All columns across registers: {all_columns}")

    if not longitudinal_data:
        logger.error("No valid longitudinal data found.")
        raise ValueError("No valid longitudinal data found.")

    logger.info("Concatenating longitudinal data from all registers")
    combined_data = pl.concat(longitudinal_data, how="diagonal")
    log_non_null_counts(combined_data, "combined_data")

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
    log_non_null_counts(child_data, "child_data")
    child_data = rename_duplicates(child_data)
    log_non_null_counts(child_data, "child_data")
    logger.debug(f"Child data schema after renaming: {child_data.collect_schema()}")
    logger.debug(f"Columns in child_data before writing: {child_data.collect_schema().names()}")

    (output_dir / "long").mkdir(parents=True, exist_ok=True)
    child_data.collect().write_parquet(output_dir / "long" / "child.parquet")

    mother_data: pl.LazyFrame | None = None
    father_data: pl.LazyFrame | None = None

    # Process mother data if exists
    if cs.expand_selector(combined_data, mother_cols):
        mother_data = combined_data.select(
            cs.by_name(["PNR", "year", "month", "MOR_ID"]) | mother_cols
        )
        if mother_data is not None:
            mother_data = mother_data.rename({"PNR": "CHILD_PNR", "MOR_ID": "MOTHER_PNR"})
            logger.debug(f"Mother data schema: {mother_data.collect_schema()}")
            (output_dir / "parent_data").mkdir(parents=True, exist_ok=True)
            mother_data.collect().write_parquet(output_dir / "long" / "mother.parquet")

    # Process father data if exists
    if cs.expand_selector(combined_data, father_cols):
        father_data = combined_data.select(
            cs.by_name(["PNR", "year", "month", "FAR_ID"]) | father_cols
        )
        if father_data is not None:
            father_data = father_data.rename({"PNR": "CHILD_PNR", "FAR_ID": "FATHER_PNR"})
            logger.debug(f"Father data schema: {father_data.collect_schema()}")
            (output_dir / "parent_data").mkdir(parents=True, exist_ok=True)
            father_data.collect().write_parquet(output_dir / "long" / "father.parquet")

    logger.debug(f"Child data sample:\n{child_data.limit(5).collect()}")
    if mother_data is not None:
        logger.debug(f"Mother data sample:\n{mother_data.limit(5).collect()}")
    if father_data is not None:
        logger.debug(f"Father data sample:\n{father_data.limit(5).collect()}")
    logger.info(f"Transformed and partitioned combined data saved to {output_dir}")
    return child_data, mother_data, father_data


def rename_duplicates(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Rename duplicate column names in a LazyFrame.

    This function appends a numeric suffix to duplicate column names to ensure
    all column names are unique.

    Args:
        df (pl.LazyFrame): The input LazyFrame with potentially duplicate column names.

    Returns:
        pl.LazyFrame: A LazyFrame with renamed columns to eliminate duplicates.

    Example:
        If a LazyFrame has columns ['A', 'B', 'A'], the output will have
        columns ['A', 'B', 'A_1'].
    """
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
    # Example usage of the process_and_partition_longitudinal_data function
    output_dir = Path("path/to/your/output/directory")

    process_and_partition_longitudinal_data(output_dir)
