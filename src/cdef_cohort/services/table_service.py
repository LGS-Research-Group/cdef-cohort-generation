from datetime import datetime
from pathlib import Path
from typing import Any, TypedDict

import pandas as pd
import polars as pl

from cdef_cohort.logging_config import logger

from .base import ConfigurableService
from .data_service import DataService


class TableData(TypedDict):
    latex: Any  # Could be str or pd.DataFrame
    html: Any  # Could be str or pd.DataFrame
    excel: pd.DataFrame
    csv: pd.DataFrame


class TableService(ConfigurableService):
    def __init__(self, data_service: DataService):
        self.data_service = data_service
        self._config: dict[str, Any] = {}

    def configure(self, config: dict[str, Any]) -> None:
        """Configure the service with provided settings"""
        self._config = config
        if not self.check_valid():
            raise ValueError("Invalid configuration")

    def initialize(self) -> None:
        """Initialize service resources"""
        if not self.check_valid():
            raise ValueError("Invalid configuration")

    def shutdown(self) -> None:
        """Clean up service resources"""
        pass  # No resources to clean up

    def check_valid(self) -> bool:
        """Validate service configuration"""
        required_configs = ["output_dir", "study_years", "analytical_data_path"]
        return all(key in self._config for key in required_configs)

    def _get_stats_file_path(self, stats_path: Path, category: str, year: int | None = None) -> Path:
        """Get the correct statistics file path based on category and year"""
        if year is not None:
            return stats_path / "longitudinal" / f"{category.lower()}_statistics_{year}.parquet"
        return stats_path / f"{category.lower()}_statistics.parquet"

    def create_table_one(self, stratify_by: str | None = None) -> TableData:
        """Create comprehensive Table 1 with descriptive statistics"""
        results = []
        stats_base_path = Path(self._config["analytical_data_path"]) / "derived" / "statistics"

        try:
            # 1. CHILD CHARACTERISTICS
            self._add_child_characteristics(stats_base_path, results, stratify_by)

            # 2. MATERNAL CHARACTERISTICS
            self._add_maternal_characteristics(stats_base_path, results, stratify_by)

            # 3. PATERNAL CHARACTERISTICS
            self._add_paternal_characteristics(stats_base_path, results, stratify_by)

            # 4. FAMILY CHARACTERISTICS
            self._add_family_characteristics(stats_base_path, results, stratify_by)

            # Convert to pandas DataFrame and create output formats
            table_df = pd.DataFrame(results)
            return self._create_table_formats(table_df)

        except Exception as e:
            logger.error(f"Error creating table one: {str(e)}")
            raise

    def _add_child_characteristics(self, stats_path: Path, results: list, stratify_by: str | None = None) -> None:
        """Add child characteristics section"""
        static_stats = self._safe_scan_parquet(stats_path / "static_statistics.parquet")
        if static_stats is None:
            return

        # Total children count
        total_stats = static_stats.filter(pl.col("column") == "role").select(pl.sum("count")).collect()
        total_count = total_stats[0, 0]
        results.append(
            {
                "Category": "Child Characteristics",
                "Variable": "Total children",
                "Value": f"{total_count:,}",
                "Missing": "0%",
                "Year": "All",
            }
        )

        # Age distribution
        self._add_numeric_stat_with_category(
            static_stats, "age", "Child Characteristics", "Age at inclusion (years)", results
        )

        # Birth year distribution
        self._add_categorical_stat_with_category(
            static_stats, "birth_year", "Child Characteristics", "Birth year distribution", results
        )

        # Sex distribution
        self._add_categorical_stat_with_category(static_stats, "sex", "Child Characteristics", "Sex", results)

    def _handle_stratification(self, stats: pl.LazyFrame, stratify_by: str | None) -> pl.LazyFrame:
        """Handle stratification of statistics"""
        if stratify_by is None:
            return stats

        return stats.filter(pl.col("stratification") == stratify_by)

    def _add_maternal_characteristics(self, stats_path: Path, results: list, stratify_by: str | None = None) -> None:
        """Add maternal characteristics section"""
        # Add maternal age
        family_stats = pl.scan_parquet(stats_path / "family_statistics.parquet")
        self._add_numeric_stat_with_category(
            family_stats, "mother_age_at_birth", "Maternal Characteristics", "Age at child birth (years)", results
        )

        # Add longitudinal characteristics
        for year in self._config["study_years"]:
            longitudinal_stats = pl.scan_parquet(stats_path / "longitudinal" / f"mother_characteristics_{year}.parquet")

            # Education
            self._add_categorical_stat_with_category(
                longitudinal_stats,
                "education_level",
                "Maternal Characteristics",
                f"Education level ({year})",
                results,
                year=year,
            )

            # Income
            self._add_numeric_stat_with_category(
                longitudinal_stats,
                "income",
                "Maternal Characteristics",
                f"Annual income ({year}) DKK",
                results,
                year=year,
            )

            # Employment
            self._add_categorical_stat_with_category(
                longitudinal_stats,
                "employment_status",
                "Maternal Characteristics",
                f"Employment status ({year})",
                results,
                year=year,
            )

    def _add_paternal_characteristics(self, stats_path: Path, results: list, stratify_by: str | None = None) -> None:
        """Add paternal characteristics section"""
        # Add paternal age
        family_stats = pl.scan_parquet(stats_path / "family_statistics.parquet")
        self._add_numeric_stat_with_category(
            family_stats, "father_age_at_birth", "Paternal Characteristics", "Age at child birth (years)", results
        )

        # Add longitudinal characteristics
        for year in self._config["study_years"]:
            longitudinal_stats = pl.scan_parquet(stats_path / "longitudinal" / f"father_characteristics_{year}.parquet")

            # Education
            self._add_categorical_stat_with_category(
                longitudinal_stats,
                "education_level",
                "Paternal Characteristics",
                f"Education level ({year})",
                results,
                year=year,
            )

            # Income
            self._add_numeric_stat_with_category(
                longitudinal_stats,
                "income",
                "Paternal Characteristics",
                f"Annual income ({year}) DKK",
                results,
                year=year,
            )

            # Employment
            self._add_categorical_stat_with_category(
                longitudinal_stats,
                "employment_status",
                "Paternal Characteristics",
                f"Employment status ({year})",
                results,
                year=year,
            )

    def _add_family_characteristics(self, stats_path: Path, results: list, stratify_by: str | None = None) -> None:
        """Add family characteristics section"""
        family_stats = pl.scan_parquet(stats_path / "family_statistics.parquet")

        # Family type
        self._add_categorical_stat_with_category(
            family_stats, "family_type", "Family Characteristics", "Family type", results
        )

        # Household size
        self._add_numeric_stat_with_category(
            family_stats, "household_size", "Family Characteristics", "Number of persons in household", results
        )

        # Number of children
        self._add_numeric_stat_with_category(
            family_stats, "number_of_children", "Family Characteristics", "Number of children in family", results
        )

    def _add_numeric_stat_with_category(
        self,
        stats: pl.LazyFrame,
        column: str,
        category: str,
        variable: str,
        results: list,
        year: int | None = None,
        stratify_by: str | None = None,
    ) -> None:
        """Add formatted numeric statistics with category"""
        stats_df = stats.filter(pl.col("column") == column).collect()

        if stats_df.is_empty():
            logger.warning(f"No statistics found for column: {column}")
            return

        row = stats_df.row(0, named=True)

        # Format the numeric values
        value = (
            f"Mean: {self.format_number(row['mean'])} "
            f"(SD: {self.format_number(row['std'])})\n"
            f"Median: {self.format_number(row['median'])} "
            f"(IQR: {self.format_number(row['q1'])}-{self.format_number(row['q3'])})"
        )

        # Calculate missing percentage
        total = row["count"] + row["missing"]
        missing_pct = (row["missing"] / total * 100) if total > 0 else 0

        results.append(
            {
                "Category": category,
                "Variable": variable,
                "Value": value,
                "Missing": f"{missing_pct:.1f}%",
                "Year": str(year) if year is not None else "All",
            }
        )

    def _add_categorical_stat_with_category(
        self,
        stats: pl.LazyFrame,
        column: str,
        category: str,
        variable: str,
        results: list,
        year: int | None = None,
        stratify_by: str | None = None,
    ) -> None:
        """Add formatted categorical statistics with category"""
        stats_df = stats.filter(pl.col("column") == column).collect()

        if stats_df.is_empty():
            logger.warning(f"No statistics found for column: {column}")
            return

        # Get category counts and percentages
        categories = stats_df.filter(pl.col(column).is_not_null())
        value_lines = []

        for row in categories.iter_rows(named=True):
            category_value = str(row[column])
            count = row["count"]
            percentage = row["percentage"]
            value_lines.append(f"{category_value}: {count:,} ({percentage:.1f}%)")

        # Calculate missing percentage
        total = stats_df["count"].sum()
        missing = stats_df.filter(pl.col(column).is_null())["count"].sum()
        missing_pct = (missing / total * 100) if total > 0 else 0

        results.append(
            {
                "Category": category,
                "Variable": variable,
                "Value": "\n".join(value_lines),
                "Missing": f"{missing_pct:.1f}%",
                "Year": str(year) if year is not None else "All",
            }
        )

    def format_number(self, value: float, decimals: int = 1) -> str:
        """Format numbers with thousands separator and specified decimals."""
        if isinstance(value, (float | int)):
            return f"{value:,.{decimals}f}"
        return str(value)

    def _add_static_characteristics(self, stats_path: Path, results: list, stratify_by: str | None = None) -> None:
        """Add characteristics from static statistics"""
        try:
            static_stats = pl.scan_parquet(stats_path / "static_statistics.parquet")

            # First get total counts from statistics by summing all counts for role
            total_stats = (
                static_stats.filter(pl.col("column") == "role").select(pl.sum("count").alias("total_count")).collect()
            )

            if not total_stats.is_empty():
                total_count = total_stats["total_count"][0]  # Get first value instead of item()
                results.append(
                    {
                        "Category": "Demographics",
                        "Variable": "Total individuals",
                        "Value": f"{total_count:,}",
                        "Missing": "0%",
                        "Year": "All",
                    }
                )

            # Process each statistic by type
            stats_df = static_stats.collect()
            for row in stats_df.iter_rows(named=True):
                column = row["column"]
                stat_type = row["stat_type"]

                if stat_type == "numeric":
                    # Format numeric statistics
                    value = f"Mean: {self.format_number(row['value'])} " f"(SD: {self.format_number(row['std'])})"
                elif stat_type == "categorical":
                    # Format categorical statistics
                    value = f"{self.format_number(row['count'])} ({self.format_number(row['value'])}%)"
                else:  # temporal
                    value = str(row["value"])

                # Calculate missing percentage
                total = row["count"] + row["missing"]
                missing_pct = (row["missing"] / total * 100) if total > 0 else 0

                results.append(
                    {
                        "Category": "Demographics",
                        "Variable": column,
                        "Value": value,
                        "Missing": f"{missing_pct:.1f}%",
                        "Year": "All",
                    }
                )

        except Exception as e:
            logger.error(f"Error adding static characteristics: {str(e)}")
            raise

    def _add_longitudinal_characteristics(
        self, stats_path: Path, results: list, stratify_by: str | None = None
    ) -> None:
        """Add characteristics from longitudinal statistics"""
        domains = ["demographics", "education", "income", "employment"]
        longitudinal_path = stats_path / "longitudinal"

        try:
            for domain in domains:
                domain_path = longitudinal_path / f"{domain}_statistics.parquet"
                if not domain_path.exists():
                    continue

                domain_stats = pl.scan_parquet(domain_path)
                stats_df = domain_stats.collect()

                for row in stats_df.iter_rows(named=True):
                    column = row["column"]
                    stat_type = row["stat_type"]
                    year = row["year"]

                    if stat_type == "numeric":
                        value = f"Mean: {self.format_number(row['value'])} " f"(SD: {self.format_number(row['std'])})"
                    elif stat_type == "categorical":
                        value = f"{self.format_number(row['count'])} ({self.format_number(row['value'])}%)"
                    else:  # temporal
                        value = str(row["value"])

                    total = row["count"] + row["missing"]
                    missing_pct = (row["missing"] / total * 100) if total > 0 else 0

                    results.append(
                        {
                            "Category": domain.capitalize(),
                            "Variable": column,
                            "Value": value,
                            "Missing": f"{missing_pct:.1f}%",
                            "Year": str(year),
                        }
                    )

        except Exception as e:
            logger.error(f"Error adding longitudinal characteristics: {str(e)}")
            raise

    def _create_table_formats(self, df: pd.DataFrame) -> TableData:
        """Create different format versions of the table."""
        return {
            "latex": self._create_latex_table(df),
            "html": self._create_html_table(df),
            "excel": df,
            "csv": df,
        }

    def _safe_scan_parquet(self, file_path: Path) -> pl.LazyFrame | None:
        """Safely scan a parquet file with error handling"""
        try:
            if not file_path.exists():
                logger.warning(f"Statistics file not found: {file_path}")
                return None
            return pl.scan_parquet(file_path)
        except Exception as e:
            logger.error(f"Error reading statistics file {file_path}: {str(e)}")
            return None

    def _create_latex_table(self, df: pd.DataFrame) -> str:
        """Convert DataFrame to LaTeX format"""
        return df.to_latex(index=False, escape=False)

    def _create_html_table(self, df: pd.DataFrame) -> str:
        """Convert DataFrame to HTML format"""
        return df.to_html(index=False)

    def save_tables(self, tables: TableData, prefix: str = "table_one") -> None:
        """Save tables in specified formats"""
        if "output_dir" not in self._config:
            raise ValueError("Output directory not configured")

        timestamp = datetime.now().strftime("%Y%m%d")
        output_dir = Path(self._config["output_dir"])

        try:
            output_dir.mkdir(parents=True, exist_ok=True)

            for format_type, table in tables.items():
                output_path = output_dir / f"{prefix}_{timestamp}.{format_type}"

                if format_type in ("latex", "html"):
                    content = (
                        (self._create_latex_table(table) if format_type == "latex" else self._create_html_table(table))
                        if isinstance(table, pd.DataFrame)
                        else str(table)
                    )

                    with open(output_path, "w", encoding="utf-8") as f:
                        f.write(content)

                elif format_type == "excel" and isinstance(table, pd.DataFrame):
                    table.to_excel(output_path, index=False)

                elif format_type == "csv" and isinstance(table, pd.DataFrame):
                    table.to_csv(output_path, index=False)

        except Exception as e:
            logger.error(f"Error saving tables: {str(e)}")
            raise
