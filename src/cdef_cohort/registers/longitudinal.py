import shutil
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import polars.selectors as cs

from cdef_cohort.logging_config import logger
from cdef_cohort.registers.base import BaseProcessor
from cdef_cohort.services.data_service import DataService
from cdef_cohort.services.event_service import EventService
from cdef_cohort.services.mapping_service import MappingService
from cdef_cohort.utils.config import (
    AKM_OUT,
    BEF_OUT,
    IDAN_OUT,
    IND_OUT,
    STATIC_COHORT,
    UDDF_OUT,
)
from cdef_cohort.utils.hash_utils import process_with_hash_check

CHUNK_SIZE = 150_000_000  # 150 million rows per chunk

if TYPE_CHECKING:
    from .factory import RegisterFactory


class LongitudinalProcessor(BaseProcessor):
    def __init__(
        self,
        data_service: DataService,
        event_service: EventService,
        mapping_service: MappingService,
        register_factory: "RegisterFactory",
    ):
        super().__init__(data_service, event_service, mapping_service)
        self.register_factory = register_factory
        logger.debug("Initialized LongitudinalProcessor")

    def estimate_memory_usage(self, df: pl.DataFrame) -> float:
        """Estimate memory usage of DataFrame in GB."""
        return df.estimated_size() / (1024**3)

    def log_non_null_counts(self, df: pl.LazyFrame, name: str) -> None:
        """Log the count of non-null values for each column in a DataFrame."""
        counts = df.select(pl.all().is_not_null().sum()).collect().to_dict()
        logger.debug(f"Non-null counts for {name}:")
        for col, count in counts.items():
            logger.debug(f"  {col}: {count[0]}")

    def validate_data_structure(
        self,
        child_dir: Path,
        mother_dir: Path,
        father_dir: Path,
    ) -> None:
        """Validate the structure of partitioned data."""
        for dir_path in [child_dir, mother_dir, father_dir]:
            if dir_path.exists():
                year_partitions = list(dir_path.glob("year=*"))
                if not year_partitions:
                    raise ValueError(f"No year partitions found in {dir_path}")
                for partition in year_partitions:
                    if not list(partition.glob("*.parquet")):
                        raise ValueError(f"Empty partition found: {partition}")

    def validate_partitions(self, data_dir: Path) -> None:
        """Validate that all expected partitions exist and are consistent."""
        if not data_dir.is_dir():
            raise ValueError(f"Directory {data_dir} does not exist")

        years = [p.name.split("=")[1] for p in data_dir.glob("year=*")]
        if not years:
            logger.warning(f"No year partitions found in {data_dir}")
            return

        years = [int(y) for y in years]
        logger.info(f"Found partitions for years: {sorted(years)}")

        year_range = range(min(years), max(years) + 1)
        missing_years = set(year_range) - set(years)

        if missing_years:
            logger.warning(f"Missing partitions for years: {missing_years}")

        for year in years:
            year_dir = data_dir / f"year={year}"
            if not list(year_dir.glob("*.parquet")):
                logger.warning(f"No parquet files found in partition: {year_dir}")

    def read_longitudinal_data(
        self,
        data_dir: Path,
        start_year: int | None = None,
        end_year: int | None = None,
    ) -> pl.LazyFrame:
        """Read longitudinal data with optional year range filter."""
        if not data_dir.exists():
            raise FileNotFoundError(f"Directory not found: {data_dir}")

        df = self.data_service.read_parquet(data_dir)

        if start_year is not None:
            df = df.filter(pl.col("year") >= start_year)
        if end_year is not None:
            df = df.filter(pl.col("year") <= end_year)

        return df

    def write_partitioned_data(
        self,
        df: pl.LazyFrame,
        output_path: Path,
        partition_by: str = "year",
    ) -> None:
        """Write data to partitioned parquet files with optimized settings."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.collect().write_parquet(
            output_path,
            partition_by=partition_by,
            use_pyarrow=True,
            compression="zstd",
            compression_level=3,
            row_group_size=512 * 512,
            data_page_size=1024 * 1024,
            statistics=True,
        )

    def process_chunk(
        self,
        data: pl.LazyFrame,
        output_dir: Path,
        chunk_size: int = CHUNK_SIZE,
    ) -> None:
        """Process and write data in chunks to avoid memory issues."""
        collected_df = data.collect()

        sample_size = min(1000, len(collected_df))
        sample_mem = self.estimate_memory_usage(collected_df.head(sample_size))
        estimated_mem_per_row = sample_mem / sample_size

        adjusted_chunk_size = int(100 / estimated_mem_per_row)
        final_chunk_size = min(chunk_size, adjusted_chunk_size)

        logger.info(f"Using chunk size of {final_chunk_size:,} rows")

        total_rows = len(collected_df)
        for i in range(0, total_rows, final_chunk_size):
            chunk = collected_df.slice(i, final_chunk_size)
            chunk_df = pl.LazyFrame(chunk)
            self.write_partitioned_data(chunk_df, output_dir)
            logger.debug(
                f"Processed chunk {i//final_chunk_size + 1}, "
                f"memory usage: {self.estimate_memory_usage(chunk):.2f}GB"
            )

    def process_register_parallel(
        self,
        registers_to_process: list[tuple[Callable[..., Any], str]],
        common_params: dict[str, Any],
    ) -> None:
        """Process registers in parallel using ThreadPoolExecutor."""
        with ThreadPoolExecutor() as executor:
            futures = []
            for process_func, register_name in registers_to_process:
                future = executor.submit(process_with_hash_check, process_func, **common_params)
                futures.append((future, register_name))

            for future, register_name in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing {register_name}: {str(e)}")
                    raise

    def rename_duplicates(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Rename duplicate column names in a LazyFrame."""
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

    def preprocess(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Required preprocess method implementation."""
        return df

    def process(self, **kwargs: Any) -> tuple[pl.LazyFrame, pl.LazyFrame | None, pl.LazyFrame | None]:
        """Process, combine, and partition longitudinal data from various registers."""
        if "output_dir" not in kwargs:
            raise ValueError("output_dir is required")

        output_dir = kwargs["output_dir"]
        long_dir = output_dir / "long"

        # Setup directories
        child_dir = long_dir / "child"
        mother_dir = long_dir / "mother"
        father_dir = long_dir / "father"

        # Check if partitioned data already exists
        if all(p.exists() for p in [child_dir, mother_dir, father_dir]):
            logger.info("Existing partitioned data found. Validating and reading from directories.")
            try:
                self.validate_data_structure(child_dir, mother_dir, father_dir)
                return (
                    self.read_longitudinal_data(child_dir),
                    self.read_longitudinal_data(mother_dir),
                    self.read_longitudinal_data(father_dir),
                )
            except ValueError as e:
                logger.warning(f"Data validation failed: {e}. Reprocessing data...")
                for dir_path in [child_dir, mother_dir, father_dir]:
                    if dir_path.exists():
                        shutil.rmtree(dir_path)

        try:
            logger.info("Processing longitudinal data")
            common_params = {
                "population_file": STATIC_COHORT,
                "longitudinal": True,
            }

            # Create processor instances
            bef_processor = self.register_factory.create("bef")
            akm_processor = self.register_factory.create("akm")
            ind_processor = self.register_factory.create("ind")
            idan_processor = self.register_factory.create("idan")
            uddf_processor = self.register_factory.create("uddf")

            # Process registers in parallel
            registers_to_process = [
                (bef_processor.process, "BEF"),
                (akm_processor.process, "AKM"),
                (ind_processor.process, "IND"),
                (idan_processor.process, "IDAN"),
                (uddf_processor.process, "UDDF"),
            ]
            self.process_register_parallel(registers_to_process, common_params)

            # Read and combine register data
            longitudinal_data = []
            register_files = [
                (BEF_OUT, "BEF"),
                (AKM_OUT, "AKM"),
                (IND_OUT, "IND"),
                (IDAN_OUT, "IDAN"),
                (UDDF_OUT, "UDDF"),
            ]

            for register_file, register_name in register_files:
                try:
                    register_data = self.data_service.read_parquet(register_file)
                    longitudinal_data.append(register_data)
                    logger.debug(f"Successfully read {register_name} data")
                except Exception as e:
                    logger.warning(f"Error reading {register_name}: {str(e)}. Skipping this register.")

            if not longitudinal_data:
                raise ValueError("No valid longitudinal data found.")

            # Combine data
            combined_data = pl.concat(longitudinal_data, how="diagonal")

            # Define column selections
            child_cols = cs.by_name(["PNR", "year", "month"]) | (cs.all() - cs.starts_with("FAR_", "MOR_"))
            mother_cols = cs.starts_with("MOR_")
            father_cols = cs.starts_with("FAR_")

            # Process child data
            logger.info("Processing child data")
            child_data = self.rename_duplicates(combined_data.select(child_cols))
            self.process_chunk(child_data, child_dir)

            # Process mother data
            result_mother = None
            if cs.expand_selector(combined_data, mother_cols):
                logger.info("Processing mother data")
                mother_data = combined_data.select(cs.by_name(["PNR", "year", "month", "MOR_ID"]) | mother_cols).rename(
                    {"PNR": "CHILD_PNR", "MOR_ID": "MOTHER_PNR"}
                )
                self.process_chunk(mother_data, mother_dir)
                result_mother = self.read_longitudinal_data(mother_dir)

            # Process father data
            result_father = None
            if cs.expand_selector(combined_data, father_cols):
                logger.info("Processing father data")
                father_data = combined_data.select(cs.by_name(["PNR", "year", "month", "FAR_ID"]) | father_cols).rename(
                    {"PNR": "CHILD_PNR", "FAR_ID": "FATHER_PNR"}
                )
                self.process_chunk(father_data, father_dir)
                result_father = self.read_longitudinal_data(father_dir)

            # Validate final data structure
            self.validate_data_structure(child_dir, mother_dir, father_dir)

            logger.info(f"Data processing completed. Data saved to {long_dir}")
            return child_data, result_mother, result_father

        except Exception as e:
            logger.error(f"Error during data processing: {str(e)}")
            # Cleanup on failure
            for dir_path in [child_dir, mother_dir, father_dir]:
                if dir_path.exists():
                    shutil.rmtree(dir_path)
            raise
