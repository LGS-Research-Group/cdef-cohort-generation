import json
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from cdef_cohort.logging_config import logger
from cdef_cohort.models.analytical_data import AnalyticalDataConfig

from .base import ConfigurableService
from .cohort_service import CohortService
from .data_service import DataService


class AnalyticalDataService(ConfigurableService):
    def __init__(self, data_service: DataService, cohort_service: CohortService):
        self.data_service = data_service
        self.cohort_service = cohort_service
        self._config: AnalyticalDataConfig | None = None
        self._stage_results: dict[str, pl.LazyFrame] = {}
        self._population_file: Path | None = None

    def configure(self, config: dict[str, Any]) -> None:
        """Configure the service with pipeline results and settings."""
        try:
            base_path = Path(config["output_base_path"])

            self._config = AnalyticalDataConfig(
                base_path=base_path,
                zones={
                    "static": base_path / "static",
                    "longitudinal": base_path / "longitudinal",
                    "family": base_path / "family",
                    "derived": base_path / "derived",
                },
            )

            self._stage_results = config.get("stage_results", {})
            self._population_file = Path(config.get("population_file", ""))

            # Create necessary directories
            self._config.base_path.mkdir(parents=True, exist_ok=True)
            for zone_path in self._config.zones.values():
                zone_path.mkdir(parents=True, exist_ok=True)

            # Perform validation with detailed error messages
            validation_errors = self._validate_configuration()
            if validation_errors:
                error_msg = "\n".join(validation_errors)
                logger.error(f"Configuration validation failed:\n{error_msg}")
                raise ValueError(f"Invalid configuration:\n{error_msg}")

        except Exception as e:
            logger.error(f"Error during configuration: {str(e)}")
            raise

    def initialize(self) -> None:
        if not self.check_valid():
            raise ValueError("Invalid configuration")

    def shutdown(self) -> None:
        self._stage_results.clear()

    def check_valid(self) -> bool:
        """Check if the service is properly configured."""
        return len(self._validate_configuration()) == 0

    def create_analytical_dataset(self) -> None:
        """Create an organized analytical dataset with different data zones."""
        logger.info("Creating analytical dataset structure")

        if self._config is None:
            raise ValueError("Configuration not set")

        try:
            base_path = self._config.base_path
            base_path.mkdir(parents=True, exist_ok=True)

            # Create data zones
            zones = {
                "static": base_path / "static",
                "longitudinal": base_path / "longitudinal",
                "family": base_path / "family",
                "derived": base_path / "derived",
            }

            for zone in zones.values():
                zone.mkdir(exist_ok=True)

            # 1. Static Zone
            static_data = self._create_static_data()
            static_data.write_parquet(zones["static"] / "individual_attributes.parquet", compression="snappy")

            # 2. Longitudinal Zone
            self._create_longitudinal_data(zones["longitudinal"])
            self._create_health_data(zones["longitudinal"])

            # 3. Family Zone
            family_data = self._create_family_data()
            family_data.write_parquet(zones["family"] / "family_relationships.parquet", compression="snappy")

            # 4. Derived Zone
            self._create_derived_data(zones["derived"])

            # Create metadata
            self._create_metadata(base_path)

            logger.info(f"Analytical dataset created at: {base_path}")

        except Exception as e:
            logger.error(f"Error creating analytical dataset: {str(e)}")
            raise

    def _create_static_data(self) -> pl.DataFrame:
        """Create static individual data."""
        if self._population_file is None:
            raise ValueError("Population file not set")

        population_df = self.data_service.read_parquet(self._population_file).collect()

        # Create static data for children
        static_data = population_df.select(
            [
                pl.col("PNR").alias("individual_id"),
                pl.col("FOED_DAG").alias("birth_date"),
                pl.lit("child").alias("role"),
            ]
        )

        # Create parent data - first for fathers
        father_data = population_df.select(
            [
                pl.col("FAR_ID").alias("individual_id"),  # Already aliased to individual_id
                pl.col("FAR_FDAG").alias("birth_date"),
                pl.lit("father").alias("role"),
            ]
        ).filter(pl.col("individual_id").is_not_null())

        # Then for mothers
        mother_data = population_df.select(
            [
                pl.col("MOR_ID").alias("individual_id"),  # Already aliased to individual_id
                pl.col("MOR_FDAG").alias("birth_date"),
                pl.lit("mother").alias("role"),
            ]
        ).filter(pl.col("individual_id").is_not_null())

        # Concatenate all data - now all columns have the same names
        return pl.concat([static_data, father_data, mother_data], how="vertical")

    def _create_longitudinal_data(self, output_path: Path) -> None:
        """Create longitudinal data by domain."""
        if not self._config:  # Add null check
            raise ValueError("Configuration not set")

        for domain in self._config.domains.values():
            if not domain.temporal:
                continue

            domain_path = output_path / domain.name
            domain_path.mkdir(exist_ok=True)

            for source in domain.sources:
                if source in self._stage_results:
                    df = self._stage_results[source]
                    self.data_service.write_parquet(df, domain_path / f"{source}.parquet", partition_by="year")

    def _create_family_data(self) -> pl.DataFrame:
        """Create family relationship data."""
        if self._population_file is None:
            raise ValueError("Population file not set")

        population_df = self.data_service.read_parquet(self._population_file).collect()

        family_data = population_df.select(
            [
                pl.col("PNR").alias("child_id"),
                pl.col("FAR_ID").alias("father_id"),
                pl.col("MOR_ID").alias("mother_id"),
                pl.col("FAMILIE_ID").alias("family_id"),
                pl.col("FOED_DAG").alias("birth_date"),
                pl.col("FAR_FDAG").alias("father_birth_date"),
                pl.col("MOR_FDAG").alias("mother_birth_date"),
            ]
        )

        return family_data

    def _create_health_data(self, output_path: Path) -> None:
        """Create health data for the analytical dataset."""
        try:
            # Create health directory in longitudinal zone
            health_path = output_path / "health"
            health_path.mkdir(exist_ok=True)

            # Create health analytical data
            self.cohort_service.create_analytical_health_data(health_path)

            # Update metadata to include health domain
            if self._config and hasattr(self._config, "domains"):
                from cdef_cohort.models.analytical_data import DataDomain

                self._config.domains["health"] = DataDomain(
                    name="health",
                    description="Healthcare utilization and diagnosis data",
                    sources=["health"],
                    temporal=True,
                )
        except Exception as e:
            logger.error(f"Error creating health data: {str(e)}")
            raise

    def _create_derived_data(self, output_path: Path) -> None:
        """Create derived and aggregated features."""
        if not self._population_file:
            raise ValueError("Population file not set")
        # Family-level aggregations
        family_stats = (
            self._create_family_data()
            .group_by("family_id")
            .agg(
                [
                    pl.count("child_id").alias("number_of_children"),
                    # Add other family-level statistics
                ]
            )
        )

        family_stats.write_parquet(output_path / "family_statistics.parquet", compression="snappy")

        # Individual temporal aggregations
        # ... add other derived features

    def _create_metadata(self, base_path: Path) -> None:
        """Create metadata documentation."""
        metadata = {
            "version": "1.0",
            "created_at": str(datetime.now()),
            "structure": {
                "static": "Non-temporal individual attributes",
                "longitudinal": "Temporal data by domain",
                "family": "Family relationships and structures",
                "derived": "Aggregated and computed features",
            },
            "domains": {
                "demographics": "Basic demographic information",
                "education": "Educational history and achievements",
                "income": "Income and financial data",
                "employment": "Employment history and status",
                "health": "Healthcare utilization and diagnosis data",
            },
        }

        with open(base_path / "metadata.json", "w") as f:
            json.dump(metadata, f, indent=2)

    def _validate_configuration(self) -> list[str]:
        """Validate configuration and return list of error messages."""
        errors = []

        if not self._config:
            errors.append("Configuration not set")

        if not self._population_file:
            errors.append("Population file not set")
        elif not self._population_file.exists():
            errors.append(f"Population file does not exist: {self._population_file}")

        # Make stage_results optional during initial configuration
        if not self._stage_results:
            logger.warning("No stage results available - will be populated during pipeline execution")

        # Validate zones configuration
        if self._config:
            for zone_name, zone_path in self._config.zones.items():
                if not isinstance(zone_path, Path):
                    errors.append(f"Invalid path for zone {zone_name}: {zone_path}")
                elif not zone_path.parent.exists():
                    errors.append(f"Parent directory for zone {zone_name} does not exist: {zone_path.parent}")

        return errors
