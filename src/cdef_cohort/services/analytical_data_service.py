import json
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from cdef_cohort.logging_config import logger
from cdef_cohort.models.analytical_data import AnalyticalDataConfig
from cdef_cohort.models.statistics import (
    CategoricalStatistic,
    NumericStatistic,
    StatisticType,
    TemporalStatistic,
)

from .base import ConfigurableService
from .cohort_service import CohortService
from .data_service import DataService


class AnalyticalDataStats:
    """Helper class for statistics calculations"""

    @staticmethod
    def calculate_numeric_stats(df: pl.LazyFrame, column: str) -> pl.LazyFrame:
        return df.select(
            [
                pl.col(column).count().alias(f"{column}_count"),
                pl.col(column).mean().alias(f"{column}_mean"),
                pl.col(column).std().alias(f"{column}_std"),
                pl.col(column).median().alias(f"{column}_median"),
                pl.col(column).quantile(0.25).alias(f"{column}_q1"),
                pl.col(column).quantile(0.75).alias(f"{column}_q3"),
                pl.col(column).min().alias(f"{column}_min"),
                pl.col(column).max().alias(f"{column}_max"),
                pl.col(column).is_null().sum().alias(f"{column}_missing"),
            ]
        )

    @staticmethod
    def calculate_categorical_stats(df: pl.LazyFrame, column: str) -> pl.LazyFrame:
        total = df.select(pl.count()).collect().item()
        return (
            df.group_by(column)
            .agg(pl.count().alias("count"))
            .with_columns(
                [(pl.col("count") / total * 100).alias("percentage"), pl.col(column).is_null().sum().alias("missing")]
            )
        )

    @staticmethod
    def calculate_temporal_stats(df: pl.LazyFrame, column: str) -> pl.LazyFrame:
        return df.select(
            [
                pl.col(column).count().alias(f"{column}_count"),
                pl.col(column).min().alias(f"{column}_min"),
                pl.col(column).max().alias(f"{column}_max"),
                pl.col(column).is_null().sum().alias(f"{column}_missing"),
            ]
        )


class AnalyticalDataService(ConfigurableService):
    def __init__(self, data_service: DataService, cohort_service: CohortService):
        self.data_service = data_service
        self.cohort_service = cohort_service
        self._config: AnalyticalDataConfig | None = None
        self._stats = AnalyticalDataStats()

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

            # 5. Generate Statistics - Add this section
            self._add_summary_statistics()

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
        """Create longitudinal data by domain"""
        if not self._config:
            raise ValueError("Configuration not set")

        for domain in self._config.domains.values():
            if not domain.temporal:
                continue

            domain_path = output_path / domain.name
            domain_path.mkdir(exist_ok=True)

            for source in domain.sources:
                if source in self._stage_results:
                    df = self._stage_results[source]

                    # Ensure year column exists
                    if "year" not in df.collect_schema():
                        logger.warning(f"Year column missing in {source} - skipping")
                        continue

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

    def _add_summary_statistics(self) -> None:
        """Calculate and store summary statistics for all zones."""
        if not self._config:
            raise ValueError("Configuration not set")

        try:
            stats_path = self._config.base_path / "derived" / "statistics"
            stats_path.mkdir(parents=True, exist_ok=True)

            # Calculate static statistics
            logger.info("Calculating static statistics")
            static_data = pl.scan_parquet(self._config.zones["static"] / "individual_attributes.parquet")
            static_stats = self._calculate_static_statistics(static_data)
            if not static_stats.collect().is_empty():
                static_stats.collect().write_parquet(stats_path / "static_statistics.parquet")

            # Calculate family statistics
            logger.info("Calculating family statistics")
            family_data = pl.scan_parquet(self._config.zones["family"] / "family_relationships.parquet")
            family_stats = self._calculate_family_statistics(family_data)
            if not family_stats.collect().is_empty():
                family_stats.collect().write_parquet(stats_path / "family_statistics.parquet")

            # Calculate longitudinal statistics
            logger.info("Calculating longitudinal statistics")
            longitudinal_path = stats_path / "longitudinal"
            longitudinal_path.mkdir(exist_ok=True)

            # Calculate stats for each domain
            for domain in self._config.domains.values():
                if domain.temporal:
                    logger.info(f"Calculating statistics for domain: {domain.name}")
                    try:
                        # Read domain data
                        domain_path = self._config.zones["longitudinal"] / domain.name
                        if not domain_path.exists():
                            logger.warning(f"No data found for domain: {domain.name}")
                            continue

                        domain_data = pl.scan_parquet(domain_path)
                        domain_stats = self._calculate_domain_statistics(domain, domain_data)

                        if not domain_stats.collect().is_empty():
                            domain_stats.collect().write_parquet(
                                longitudinal_path / f"{domain.name}_statistics.parquet", partition_by="year"
                            )
                    except Exception as e:
                        logger.error(f"Error processing domain {domain.name}: {str(e)}")
                        continue

        except Exception as e:
            logger.error(f"Error calculating summary statistics: {str(e)}")
            raise

    def _calculate_static_statistics(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Calculate statistics for static attributes."""
        numeric_cols = ["age"]
        categorical_cols = ["sex", "role"]
        temporal_cols = ["birth_date"]

        all_stats = []

        # For numeric columns
        for col in numeric_cols:
            if col in data.collect_schema():
                data = data.with_columns(pl.col(col).cast(pl.Float64))  # Ensure column is Float64
                stats = self.calculate_numeric_statistics(data, col)
                stats_df = pl.DataFrame(
                    {
                        "column": [col],
                        "stat_type": ["numeric"],
                        "count": [stats.count],
                        "value": [stats.mean],
                        "std": [stats.std],
                        "missing": [stats.missing],
                    }
                )
                all_stats.append(stats_df.lazy())

        # For categorical columns
        for col in categorical_cols:
            if col in data.collect_schema():
                stats = self.calculate_categorical_statistics(data, col)
                stats_df = pl.DataFrame(
                    {
                        "column": [col],
                        "stat_type": ["categorical"],
                        "count": [sum(stats.categories.values())],
                        "value": [max(stats.percentages.values())],
                        "std": [0.0],  # Changed from None to 0.0
                        "missing": [stats.missing],
                    }
                )
                all_stats.append(stats_df.lazy())

        # For temporal columns
        for col in temporal_cols:
            if col in data.collect_schema():
                data = data.with_columns(pl.col(col).cast(pl.Date))  # Ensure column is Date
                stats = self.calculate_temporal_statistics(data, col)
                stats_df = pl.DataFrame(
                    {
                        "column": [col],
                        "stat_type": ["temporal"],
                        "count": [stats.count],
                        "value": [stats.min],  # Convert to numeric representation
                        "std": [0.0],  # Changed from None to 0.0
                        "missing": [stats.missing],
                    }
                )
                all_stats.append(stats_df.lazy())

        return (
            pl.concat(all_stats) if all_stats else pl.LazyFrame([])
        )  # Return an empty LazyFrame if all_stats is empty

    def _calculate_family_statistics(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Calculate statistics for family relationships."""
        numeric_cols = ["household_size", "number_of_children"]
        categorical_cols = ["family_type"]
        temporal_cols = ["father_birth_date", "mother_birth_date"]

        all_stats = []

        # For numeric columns
        for col in numeric_cols:
            if col in data.collect_schema():
                data = data.with_columns(pl.col(col).cast(pl.Float64))  # Ensure column is Float64
                stats = self.calculate_numeric_statistics(data, col)
                stats_df = pl.DataFrame(
                    {
                        "column": [col],
                        "stat_type": ["numeric"],
                        "count": [stats.count],
                        "value": [stats.mean],
                        "std": [stats.std],
                        "missing": [stats.missing],
                    }
                )
                all_stats.append(stats_df.lazy())

        # For categorical columns
        for col in categorical_cols:
            if col in data.collect_schema():
                stats = self.calculate_categorical_statistics(data, col)
                stats_df = pl.DataFrame(
                    {
                        "column": [col],
                        "stat_type": ["categorical"],
                        "count": [sum(stats.categories.values())],
                        "value": [max(stats.percentages.values())],
                        "std": [0.0],
                        "missing": [stats.missing],
                    }
                )
                all_stats.append(stats_df.lazy())

        # For temporal columns
        for col in temporal_cols:
            if col in data.collect_schema():
                data = data.with_columns(pl.col(col).cast(pl.Date))  # Ensure column is Date
                stats = self.calculate_temporal_statistics(data, col)
                stats_df = pl.DataFrame(
                    {
                        "column": [col],
                        "stat_type": ["temporal"],
                        "count": [stats.count],
                        "value": [stats.min],
                        "std": [0.0],
                        "missing": [stats.missing],
                    }
                )
                all_stats.append(stats_df.lazy())

        return (
            pl.concat(all_stats) if all_stats else pl.LazyFrame([])
        )  # Return an empty LazyFrame if all_stats is empty

    def _calculate_domain_statistics(self, domain: Any, data: pl.LazyFrame) -> pl.LazyFrame:
        """Calculate statistics for a specific domain."""
        # First check if data has a year column
        schema = data.collect_schema()
        if "year" not in schema:
            logger.warning(f"No year column found in data for domain {domain.name}")
            return pl.LazyFrame()  # Return empty frame if no year column

        stats_config = {
            "demographics": {"numeric": ["household_size"], "categorical": ["municipality", "region"], "temporal": []},
            "education": {"numeric": ["education_level"], "categorical": ["education_field"], "temporal": []},
            "income": {"numeric": ["annual_income", "disposable_income"], "categorical": [], "temporal": []},
            "employment": {"numeric": [], "categorical": ["employment_status", "sector"], "temporal": []},
        }

        domain_config = stats_config.get(domain.name, {"numeric": [], "categorical": [], "temporal": []})
        all_stats = []

        try:
            # Safely get unique years
            years = data.select(pl.col("year")).unique().collect().get_column("year").to_list()

            if not years:
                logger.warning(f"No years found in data for domain {domain.name}")
                return pl.LazyFrame()

            for year in years:
                year_data = data.filter(pl.col("year") == year)
                year_stats = []

                # For numeric columns
                for col in domain_config["numeric"]:
                    if col in year_data.collect_schema():
                        year_data = year_data.with_columns(pl.col(col).cast(pl.Float64))  # Ensure column is Float64
                        stats = self.calculate_numeric_statistics(year_data, col)
                        stats_df = pl.DataFrame(
                            {
                                "column": [col],
                                "stat_type": ["numeric"],
                                "count": [stats.count],
                                "value": [stats.mean],
                                "std": [stats.std],
                                "missing": [stats.missing],
                                "year": [year],
                            }
                        )
                        year_stats.append(stats_df.lazy())

                # For categorical columns
                for col in domain_config["categorical"]:
                    if col in year_data.collect_schema():
                        stats = self.calculate_categorical_statistics(year_data, col)
                        stats_df = pl.DataFrame(
                            {
                                "column": [col],
                                "stat_type": ["categorical"],
                                "count": [sum(stats.categories.values())],
                                "value": [max(stats.percentages.values())],
                                "std": [0.0],
                                "missing": [stats.missing],
                                "year": [year],
                            }
                        )
                        year_stats.append(stats_df.lazy())

                if year_stats:
                    all_stats.extend(year_stats)

            return pl.concat(all_stats) if all_stats else pl.LazyFrame()

        except Exception as e:
            logger.error(f"Error calculating domain statistics for {domain.name}: {str(e)}")
            return pl.LazyFrame()

    def calculate_numeric_statistics(self, df: pl.LazyFrame, column: str) -> NumericStatistic:
        """Calculate numeric statistics for a column"""
        try:
            stats = df.select(
                [
                    pl.col(column).count().alias("count"),
                    pl.col(column).mean().alias("mean"),
                    pl.col(column).std().alias("std"),
                    pl.col(column).median().alias("median"),
                    pl.col(column).quantile(0.25).alias("q1"),
                    pl.col(column).quantile(0.75).alias("q3"),
                    pl.col(column).min().alias("min"),
                    pl.col(column).max().alias("max"),
                    pl.col(column).is_null().sum().alias("missing"),
                ]
            ).collect()

            return NumericStatistic(
                count=stats["count"][0],
                mean=stats["mean"][0],
                std=stats["std"][0],
                median=stats["median"][0],
                q1=stats["q1"][0],
                q3=stats["q3"][0],
                min=stats["min"][0],
                max=stats["max"][0],
                missing=stats["missing"][0],
            )
        except Exception as e:
            logger.error(f"Error calculating numeric statistics for {column}: {e}")
            raise

    def calculate_categorical_statistics(self, df: pl.LazyFrame, column: str) -> CategoricalStatistic:
        """Calculate categorical statistics for a column"""
        try:
            total = df.select(pl.count()).collect().item()

            value_counts = df.group_by(column).agg(pl.count().alias("count")).sort("count", descending=True).collect()

            categories = {
                str(val): count
                for val, count in zip(
                    value_counts[column].to_list(),
                    value_counts["count"].to_list(),
                    strict=False,
                )
            }

            percentages = {str(val): count / total * 100 for val, count in categories.items()}

            missing = df.select(pl.col(column).is_null().sum()).collect().item()

            return CategoricalStatistic(categories=categories, percentages=percentages, missing=missing, total=total)
        except Exception as e:
            logger.error(f"Error calculating categorical statistics for {column}: {e}")
            raise

    def calculate_temporal_statistics(self, df: pl.LazyFrame, column: str) -> TemporalStatistic:
        """Calculate temporal statistics for a column"""
        try:
            stats = df.select(
                [
                    pl.col(column).count().alias("count"),
                    pl.col(column).min().alias("min"),
                    pl.col(column).max().alias("max"),
                    pl.col(column).is_null().sum().alias("missing"),
                ]
            ).collect()

            return TemporalStatistic(
                count=stats["count"][0],
                min=str(stats["min"][0]),
                max=str(stats["max"][0]),
                missing=stats["missing"][0],
            )
        except Exception as e:
            logger.error(f"Error calculating temporal statistics for {column}: {e}")
            raise

    def get_column_type(self, df: pl.LazyFrame, column: str) -> StatisticType:
        """Determine the type of statistics to calculate for a column"""
        schema = df.collect_schema()
        dtype = schema[column]

        if dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8]:
            return StatisticType.NUMERIC
        elif dtype in [pl.Date, pl.Datetime]:
            return StatisticType.TEMPORAL
        else:
            return StatisticType.CATEGORICAL
