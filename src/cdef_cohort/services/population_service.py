from pathlib import Path
from typing import Any

import polars as pl

from cdef_cohort.logging_config import logger
from cdef_cohort.utils.date import parse_dates  # Add this import

from .base import ConfigurableService
from .data_service import DataService


class PopulationService(ConfigurableService):
    def __init__(self, data_service: DataService):
        self.data_service = data_service
        self._config: dict[str, Any] = {}

    def initialize(self) -> None:
        if not self.check_valid():
            raise ValueError("Invalid configuration")

    def shutdown(self) -> None:
        pass

    def configure(self, config: dict[str, Any]) -> None:
        self._config = config
        if not self.check_valid():
            raise ValueError("Invalid configuration")

    def check_valid(self) -> bool:
        required_configs = [
            "bef_files",
            "mfr_files",
            "population_file",
            "birth_inclusion_start_year",
            "birth_inclusion_end_year",
        ]
        return all(key in self._config for key in required_configs)

    def read_bef_data(self) -> pl.LazyFrame:
        """Read BEF data and return a LazyFrame with standardized columns."""
        logger.info(f"Reading BEF data from: {self._config['bef_files']}")

        df = pl.read_parquet(
            self._config["bef_files"], columns=["PNR", "FAR_ID", "MOR_ID", "FAMILIE_ID", "FOED_DAG"]
        ).lazy()

        return df.with_columns([parse_dates("FOED_DAG")])

    def read_mfr_data(self) -> pl.LazyFrame:
        """Read MFR data and return a LazyFrame with standardized columns."""
        logger.info(f"Reading MFR data from: {self._config['mfr_files']}")

        return (
            pl.scan_parquet(self._config["mfr_files"])
            .select(["CPR_BARN", "CPR_FADER", "CPR_MODER", "FOEDSELSDATO"])
            .with_columns([parse_dates("FOEDSELSDATO")])
            .select(
                [
                    pl.col("CPR_BARN").alias("PNR"),
                    pl.col("CPR_FADER").alias("FAR_ID"),
                    pl.col("CPR_MODER").alias("MOR_ID"),
                    pl.col("FOEDSELSDATO").alias("FOED_DAG"),
                    pl.lit(None).cast(pl.Utf8).alias("FAMILIE_ID"),
                ]
            )
        )

    def get_unique_children(self, df: pl.LazyFrame) -> pl.DataFrame:
        """Filter and get unique children from the data."""
        return (
            df.filter(
                (pl.col("FOED_DAG").dt.year() >= self._config["birth_inclusion_start_year"])
                & (pl.col("FOED_DAG").dt.year() <= self._config["birth_inclusion_end_year"]),
            )
            .select(["PNR", "FOED_DAG", "FAR_ID", "MOR_ID", "FAMILIE_ID"])
            .group_by("PNR")
            .agg(
                [
                    pl.col("FOED_DAG").first(),
                    pl.col("FAR_ID").first(),
                    pl.col("MOR_ID").first(),
                    pl.col("FAMILIE_ID").first(),
                ]
            )
            .collect()
        )

    def _create_data_summary(self, df: pl.DataFrame, prefix: str) -> dict[str, int]:
        """Create summary statistics for a dataset."""
        return {
            f"total_{prefix}_records": len(df),
            f"{prefix}_missing_far": df["FAR_ID"].null_count(),
            f"{prefix}_missing_mor": df["MOR_ID"].null_count(),
        }

    def _combine_children_data(
        self, bef_children: pl.DataFrame, mfr_children: pl.DataFrame
    ) -> tuple[pl.DataFrame, dict[str, int], dict[str, int]]:
        """Combine BEF and MFR children data."""
        # Create summaries before merge
        summary_before = {
            **self._create_data_summary(bef_children, "bef"),
            **self._create_data_summary(mfr_children, "mfr"),
        }

        # Combine data
        combined = (
            bef_children.join(mfr_children, on="PNR", how="full", suffix="_mfr")
            .with_columns(
                [
                    pl.coalesce("FAR_ID", "FAR_ID_mfr").alias("FAR_ID"),
                    pl.coalesce("MOR_ID", "MOR_ID_mfr").alias("MOR_ID"),
                    pl.coalesce("FOED_DAG", "FOED_DAG_mfr").alias("FOED_DAG"),
                    pl.col("FAMILIE_ID"),
                ]
            )
            .drop(["FAR_ID_mfr", "MOR_ID_mfr", "FOED_DAG_mfr", "FAMILIE_ID_mfr"])
        )

        # Create summary after merge
        summary_after = {
            "total_combined_records": len(combined),
            "combined_missing_far": combined["FAR_ID"].null_count(),
            "combined_missing_mor": combined["MOR_ID"].null_count(),
            "records_only_in_bef": len(combined.filter(pl.col("FAMILIE_ID").is_not_null())),
            "records_only_in_mfr": len(combined.filter(pl.col("FAMILIE_ID").is_null() & pl.col("PNR").is_not_null())),
        }

        return combined, summary_before, summary_after

    def _process_parents(self, bef_data: pl.LazyFrame) -> pl.DataFrame:
        """Process parent information from BEF data."""
        return bef_data.select(["PNR", "FOED_DAG"]).group_by("PNR").agg([pl.col("FOED_DAG").first()]).collect()

    def _create_family_data(self, children: pl.DataFrame, parents: pl.DataFrame) -> pl.DataFrame:
        """Create final family dataset with parent information."""
        # Join with fathers
        family = children.join(
            parents.rename({"PNR": "FAR_ID", "FOED_DAG": "FAR_FDAG"}),
            on="FAR_ID",
            how="left",
        )

        # Join with mothers
        family = family.join(
            parents.rename({"PNR": "MOR_ID", "FOED_DAG": "MOR_FDAG"}),
            on="MOR_ID",
            how="left",
        )

        # Select final columns
        return family.select(
            [
                "PNR",
                "FOED_DAG",
                "FAR_ID",
                "FAR_FDAG",
                "MOR_ID",
                "MOR_FDAG",
                "FAMILIE_ID",
            ]
        )

    def process_population(self) -> None:
        """Process population data and save results."""
        logger.info("Starting population processing")

        # Read data
        bef_data = self.read_bef_data()
        mfr_data = self.read_mfr_data()

        # Process children
        bef_children = self.get_unique_children(bef_data)
        mfr_children = self.get_unique_children(mfr_data)

        # Combine children data
        combined_children, summary_before, summary_after = self._combine_children_data(bef_children, mfr_children)

        # Process parents and create final family data
        parents = self._process_parents(bef_data)
        family = self._create_family_data(combined_children, parents)

        # Save results
        output_dir = Path(self._config["population_file"]).parent
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save summaries and final dataset
        summary_before_df = pl.DataFrame(summary_before).lazy()
        summary_after_df = pl.DataFrame(summary_after).lazy()
        family_lazy = pl.LazyFrame(family)

        self.data_service.write_parquet(summary_before_df, output_dir / "population_summary_before.parquet")
        self.data_service.write_parquet(summary_after_df, output_dir / "population_summary_after.parquet")
        self.data_service.write_parquet(family_lazy, self._config["population_file"])

        logger.info("Population processing completed")
