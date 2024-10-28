from pathlib import Path
from typing import Any

import polars as pl

from cdef_cohort.logging_config import logger
from cdef_cohort.utils.date import parse_dates

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

        return (
            pl.read_parquet(
                self._config["bef_files"],
                columns=["PNR", "FAR_ID", "MOR_ID", "FAMILIE_ID", "FOED_DAG"],
                schema={
                    "PNR": pl.Utf8,
                    "FAR_ID": pl.Utf8,
                    "MOR_ID": pl.Utf8,
                    "FAMILIE_ID": pl.Utf8,
                    "FOED_DAG": pl.Utf8,
                },
            )
            .lazy()
            .with_columns([parse_dates("FOED_DAG")])
        )

    def read_mfr_data(self) -> pl.LazyFrame | None:
        """Read MFR data and return a LazyFrame with standardized columns, or None if no data."""
        logger.info(f"Reading MFR data from: {self._config['mfr_files']}")

        try:
            # Check if files exist
            mfr_path = Path(self._config["mfr_files"])
            if not list(mfr_path.parent.glob(mfr_path.name)):
                logger.warning("No MFR files found - continuing without MFR data")
                return None

            return (
                pl.read_parquet(
                    self._config["mfr_files"],
                    columns=["CPR_BARN", "CPR_FADER", "CPR_MODER", "FOEDSELSDATO"],
                    schema={
                        "CPR_BARN": pl.Utf8,
                        "CPR_FADER": pl.Utf8,
                        "CPR_MODER": pl.Utf8,
                        "FOEDSELSDATO": pl.Utf8,
                    },
                )
                .lazy()
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
        except Exception as e:
            logger.warning(f"Error reading MFR data: {str(e)} - continuing without MFR data")
            return None

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
        self, bef_children: pl.DataFrame, mfr_children: pl.DataFrame | None
    ) -> tuple[pl.DataFrame, dict[str, int], dict[str, int]]:
        """Combine BEF and MFR children data."""
        if mfr_children is None:
            # If no MFR data, return BEF data only with appropriate summaries
            summary_before = self._create_data_summary(bef_children, "bef")
            summary_after = {
                "total_combined_records": len(bef_children),
                "combined_missing_far": bef_children["FAR_ID"].null_count(),
                "combined_missing_mor": bef_children["MOR_ID"].null_count(),
                "records_only_in_bef": len(bef_children),
                "records_only_in_mfr": 0,
            }
            return bef_children, summary_before, summary_after

        # Original combine logic for when we have both datasets
        summary_before = {
            **self._create_data_summary(bef_children, "bef"),
            **self._create_data_summary(mfr_children, "mfr"),
        }

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

    def process_population(self) -> pl.LazyFrame:
        """Process population data and return results."""
        logger.info("Starting population processing")

        # Read data
        bef_data = self.read_bef_data()
        mfr_data = self.read_mfr_data()

        # Process children from BEF data
        bef_children = self.get_unique_children(bef_data)

        if mfr_data is not None:
            # If we have MFR data, process and combine it
            mfr_children = self.get_unique_children(mfr_data)
            combined_children, summary_before, summary_after = self._combine_children_data(bef_children, mfr_children)
        else:
            # If no MFR data, just use BEF data
            logger.info("Processing without MFR data - using BEF data only")
            combined_children = bef_children
            summary_before = self._create_data_summary(bef_children, "bef")
            summary_after = {
                "total_combined_records": len(bef_children),
                "combined_missing_far": bef_children["FAR_ID"].null_count(),
                "combined_missing_mor": bef_children["MOR_ID"].null_count(),
                "records_only_in_bef": len(bef_children),
                "records_only_in_mfr": 0,
            }

        # Process parents and create final family data
        parents = self._process_parents(bef_data)
        family = self._create_family_data(combined_children, parents)

        # Save summaries
        output_dir = Path(self._config["population_file"]).parent
        output_dir.mkdir(parents=True, exist_ok=True)

        pl.DataFrame(summary_before).write_parquet(output_dir / "population_summary_before.parquet")
        pl.DataFrame(summary_after).write_parquet(output_dir / "population_summary_after.parquet")

        # Convert to LazyFrame before returning
        return pl.LazyFrame(family)
