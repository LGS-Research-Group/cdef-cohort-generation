from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any, TypedDict

import pandas as pd
import polars as pl
import polars.selectors as cs

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

    def initialize(self) -> None:
        """Initialize service resources"""
        if not self.check_valid():
            raise ValueError("Invalid configuration")

    def shutdown(self) -> None:
        """Clean up service resources"""
        pass

    def configure(self, config: dict[str, Any]) -> None:
        """Configure the service with provided settings"""
        self._config = config
        if not self.check_valid():
            raise ValueError("Invalid configuration")

    def check_valid(self) -> bool:
        """Validate service configuration"""
        required_configs = ["output_dir", "study_years"]
        return all(key in self._config for key in required_configs)

    def format_number(self, value: Any, decimals: int = 1) -> str:
        """Format numbers with thousands separator and specified decimals."""
        if isinstance(value, pl.Series | list | dict):
            # Handle value counts or other complex types
            return str(value)
        try:
            return f"{float(value):,.{decimals}f}"
        except (ValueError, TypeError):
            return str(value)

    def calculate_stats(
        self, df: pl.LazyFrame, column: str, strata_name: str | None = None
    ) -> dict[str, dict[str, Any]]:
        """Calculate summary statistics for a given column based on its data type."""
        try:
            # Get column type
            col_type = df.collect_schema()[column]

            if col_type in [pl.Utf8, pl.Categorical]:
                # For categorical/string columns, calculate value counts and percentages
                total_count = df.select(pl.count()).collect().item()

                value_counts = (
                    df.select(pl.col(column))
                    .filter(pl.col(column).is_not_null())
                    .group_by(column)
                    .agg(pl.count().alias("count"))
                    .sort("count", descending=True)
                    .collect()
                )

                # Convert to dictionary with formatted strings
                counts_dict = {
                    str(val): f"{count:,} ({count/total_count*100:.1f}%)"
                    for val, count in zip(value_counts[column].to_list(), value_counts["count"].to_list(), strict=True)
                }

                missing_count = df.select(pl.col(column).is_null().sum()).collect().item()

                if strata_name:
                    stats = (
                        df.filter(pl.col(strata_name).is_not_null())
                        .group_by(strata_name)
                        .agg([pl.col(column).is_null().sum().alias("missing")])
                        .collect()
                    )

                    result = {}
                    for row in stats.iter_rows(named=True):
                        stratum = str(row[strata_name])
                        result[stratum] = {"value_counts": counts_dict, "missing": row["missing"]}
                    return result
                else:
                    return {"all": {"value_counts": counts_dict, "missing": missing_count}}

            # Rest of the method remains the same...
            elif col_type in [pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8]:
                summary_exprs = [
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
            elif col_type in [pl.Date, pl.Datetime]:
                summary_exprs = [
                    pl.col(column).count().alias("count"),
                    pl.col(column).min().alias("min"),
                    pl.col(column).max().alias("max"),
                    pl.col(column).is_null().sum().alias("missing"),
                ]
            else:
                summary_exprs = [pl.col(column).count().alias("count"), pl.col(column).is_null().sum().alias("missing")]

            if strata_name:
                stats = df.filter(pl.col(strata_name).is_not_null()).group_by(strata_name).agg(summary_exprs).collect()

                result = {}
                for row in stats.iter_rows(named=True):
                    stratum = str(row[strata_name])
                    stat_dict = {name: row[name] for name in stats.columns if name != strata_name}
                    result[stratum] = stat_dict
                return result
            else:
                stats = df.select(summary_exprs).collect()
                return {"all": {name: stats[name][0] for name in stats.columns}}

        except Exception as e:
            logger.error(f"Error calculating statistics for column {column}: {str(e)}")
            return {
                "all": {
                    "count": df.select(pl.count(column)).collect().item(),
                    "missing": df.select(pl.col(column).is_null().sum()).collect().item(),
                    "value_counts": {},
                }
            }

    def create_table_one(self, df: pl.LazyFrame, stratify_by: str | None = None) -> TableData:
        """Create Table 1 with descriptive statistics"""
        # Prepare data first
        df = self._prepare_data(df)

        # Validate required columns are present
        required_columns = {"PNR", "FOED_DAG"}  # Add other required columns
        if stratify_by:
            required_columns.add(stratify_by)

        available_columns = set(df.collect_schema().keys())
        missing_columns = required_columns - available_columns

        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}. " f"Available columns: {available_columns}")

        study_years = self._config.get("study_years", [2005, 2010, 2015, 2020])

        results = []
        n_total = df.select(pl.n_unique("PNR")).collect().item()

        # Add sections (Child, Maternal, Paternal characteristics)
        self._add_child_characteristics(df, results, n_total, stratify_by)
        self._add_family_characteristics(df, results, n_total, stratify_by)
        self._add_parental_characteristics(df, results, n_total, "Mother", stratify_by, study_years)
        self._add_parental_characteristics(df, results, n_total, "Father", stratify_by, study_years)

        # Convert to pandas DataFrame
        table_df = pd.DataFrame(results)

        # Create output in different formats
        tables: TableData = {
            "latex": self._create_latex_table(table_df),
            "html": self._create_html_table(table_df),
            "excel": table_df,
            "csv": table_df,
        }

        return tables

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

    def _prepare_data(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Prepare and validate data for table generation"""
        logger.info("Preparing data for table generation")

        # Get required data from different domains
        population_file = Path(self._config["population_file"])
        if not population_file.exists():
            raise FileNotFoundError(f"Population file not found: {population_file}")

        # Read base population data
        population_data = pl.scan_parquet(population_file)

        # Join with the input data
        prepared_data = population_data.join(df, on="PNR", how="left").with_columns(
            [
                pl.col("FOED_DAG").cast(pl.Date),
                pl.when(pl.col("is_scd").is_null()).then(False).otherwise(pl.col("is_scd")).alias("is_scd"),
            ]
        )

        # Log available columns
        logger.debug(f"Columns after preparation: {prepared_data.collect_schema()}")

        return prepared_data

    def _get_latest_data(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Get the latest available data for each individual"""
        return df.group_by("PNR").agg(
            [pl.col("*").exclude(["PNR", "year"]).last(), pl.col("year").max().alias("last_year")]
        )

    def _create_latex_table(self, df: pd.DataFrame) -> str:
        """Convert DataFrame to LaTeX format"""
        return df.to_latex(index=False, escape=False)

    def _create_html_table(self, df: pd.DataFrame) -> str:
        """Convert DataFrame to HTML format"""
        return df.to_html(index=False)

    def _add_characteristic(
        self,
        df: pl.LazyFrame,
        results: list,
        category: str,
        variable: str,
        column: str,
        n_total: int,
        stratify_by: str | None = None,
        formatter: Callable | None = None,
    ) -> None:
        """Helper method to add characteristics with proper error handling"""
        try:
            if column not in df.collect_schema():
                logger.warning(f"Column {column} not found - skipping {variable}")
                return

            stats = self.calculate_stats(df, column, stratify_by)
            for stratum, stat in stats.items():
                try:
                    if "value_counts" in stat:
                        # Handle categorical variables
                        value_counts = stat["value_counts"]
                        if value_counts:
                            value = "\n".join(f"{k}: {v}" for k, v in value_counts.items())
                        else:
                            value = "No categories found"
                    elif formatter:
                        value = formatter(stat)
                    else:
                        # Handle numeric statistics (unchanged)
                        if all(k in stat for k in ["mean", "std", "median", "q1", "q3"]):
                            value = (
                                f"Mean: {self.format_number(stat['mean'])} "
                                f"(SD: {self.format_number(stat['std'])})\n"
                                f"Median: {self.format_number(stat['median'])} "
                                f"(IQR: {self.format_number(stat['q1'])}-"
                                f"{self.format_number(stat['q3'])})"
                            )
                        elif all(k in stat for k in ["min", "max"]):
                            value = f"Range: {stat['min']} to {stat['max']}"
                        else:
                            value = f"Count: {int(float(stat['count'])):,}"

                    results.append(
                        {
                            "Category": category,
                            "Variable": f"{variable}{f' - {stratum}' if stratum != 'all' else ''}",
                            "Value": value,
                            "Missing": f"{(float(stat['missing'])/n_total*100):.1f}%",
                        }
                    )
                except Exception as e:
                    logger.warning(f"Error formatting statistics for {variable} ({column}): {str(e)}")
        except Exception as e:
            logger.warning(f"Error processing {variable} ({column}): {str(e)}")

    def _format_date(self, date_val: Any) -> str:
        """Format date values consistently."""
        try:
            if date_val is None:
                return "N/A"
            if isinstance(date_val, pl.Series):
                if len(date_val) == 0:
                    return "N/A"
                return date_val.cast(pl.Date).dt.strftime("%Y-%m-%d")[0]
            if isinstance(date_val, pl.Date):
                return pl.Series([date_val]).cast(pl.Date).dt.strftime("%Y-%m-%d")[0]
            return str(date_val)
        except Exception as e:
            logger.warning(f"Error formatting date value: {str(e)}")
            return "N/A"

    # Helper methods for adding different sections of the table
    def _add_child_characteristics(
        self, df: pl.LazyFrame, results: list, n_total: int, stratify_by: str | None
    ) -> None:
        # Add total children
        results.append(
            {
                "Category": "Child Characteristics",
                "Variable": "Total children",
                "Value": f"{n_total:,}",
                "Missing": "0%",
            }
        )

        # Child demographics using selectors
        demo_cols = cs.categorical() | cs.contains("KOEN")
        for col in df.select(demo_cols).collect_schema().keys():
            self._add_characteristic(
                df,
                results,
                "Child Characteristics",
                f"{col}",
                col,
                n_total,
                stratify_by,
            )

        # Birth date and other temporal characteristics
        temporal_cols = cs.temporal() & ~cs.duration()
        for col in df.select(temporal_cols).collect_schema().keys():
            self._add_characteristic(
                df,
                results,
                "Child Characteristics",
                f"{col}",
                col,
                n_total,
                stratify_by,
                formatter=lambda stat: f"Range: {self._format_date(stat['min'])} to {self._format_date(stat['max'])}",
            )

        # SCD status and diagnosis
        scd_count = df.filter(pl.col("is_scd")).select(pl.count()).collect().item()
        results.append(
            {
                "Category": "Child Characteristics",
                "Variable": "SCD Status",
                "Value": f"{scd_count:,} ({(scd_count/n_total*100):.1f}%)",
                "Missing": f"{(df.filter(pl.col('is_scd').is_null()).select(
                    pl.count()).collect().item() / n_total * 100):.1f}%",
            }
        )

        # Age at SCD diagnosis for those with SCD
        if scd_count > 0:
            scd_age_stats = self.calculate_stats(df.filter(pl.col("is_scd")), "first_scd_date", stratify_by)
            for stratum, stats in scd_age_stats.items():
                results.append(
                    {
                        "Category": "Child Characteristics",
                        "Variable": f"Age at SCD diagnosis{f' - {stratum}' if stratum != 'all' else ''}",
                        "Value": f"Range: {self._format_date(stats['min'])} to {self._format_date(stats['max'])}",
                        "Missing": f"{(stats['missing']/scd_count*100):.1f}%",
                    }
                )

    def _add_family_characteristics(
        self, df: pl.LazyFrame, results: list, n_total: int, stratify_by: str | None
    ) -> None:
        """Add family characteristics section to results"""
        # Family structure
        family_stats = (
            df.group_by("FAMILIE_TYPE")
            .agg(pl.count().alias("count"))
            .with_columns((pl.col("count") / n_total * 100).alias("percentage"))
            .collect()
        )

        for row in family_stats.iter_rows():
            results.append(
                {
                    "Category": "Family Characteristics",
                    "Variable": f"Family structure - {row[0]}",
                    "Value": f"{row[1]:,} ({row[2]:.1f}%)",
                    "Missing": f"{(df.filter(
                        pl.col('FAMILIE_TYPE').is_null()).select(pl.count()).collect().item() / n_total * 100):.1f}%",
                }
            )

        # Number of siblings and children
        for col in ["ANTBOERNF", "ANTPERSH"]:
            stats = self.calculate_stats(df, col, stratify_by)
            for stratum, stat in stats.items():
                results.append(
                    {
                        "Category": "Family Characteristics",
                        "Variable": (
                            f"{'Number of children in family' if col == 'ANTBOERNF' else 'Household size'}"
                            f"{f' - {stratum}' if stratum != 'all' else ''}"
                        ),
                        "Value": (
                            f"Mean: {self.format_number(stat['mean'])} "
                            f"(SD: {self.format_number(stat['std'])})\n"
                            f"Median: {self.format_number(stat['median'])} "
                            f"(IQR: {self.format_number(stat['q1'])}-{self.format_number(stat['q3'])})"
                        ),
                        "Missing": f"{(stat['missing']/n_total*100):.1f}%",
                    }
                )

        # Municipality/Region distribution
        for geo_col in ["KOM", "REG"]:
            geo_stats = (
                df.group_by(geo_col)
                .agg(pl.count().alias("count"))
                .with_columns((pl.col("count") / n_total * 100).alias("percentage"))
                .sort("count", descending=True)
                .collect()
                .head(5)  # Get top 5 directly
            )

            top_5_values = "\n".join(
                f"{val}: {count:,} ({pct:.1f}%)"
                for val, count, pct in zip(
                    geo_stats[geo_col].to_list(),
                    geo_stats["count"].to_list(),
                    geo_stats["percentage"].to_list(),
                    strict=False,
                )
            )

            results.append(
                {
                    "Category": "Family Characteristics",
                    "Variable": "Municipality" if geo_col == "KOM" else "Region",
                    "Value": f"Top 5 most common:\n{top_5_values}",
                    "Missing": (
                        f"{(df.filter(pl.col(geo_col).is_null()).select(
                            pl.count()).collect().item() / n_total * 100):.1f}%"
                    ),
                }
            )

    def _add_parental_characteristics(
        self,
        df: pl.LazyFrame,
        results: list,
        n_total: int,
        parent_type: str,
        stratify_by: str | None,
        study_years: list[int],
    ) -> None:
        """Add parental characteristics section to results.

        Args:
            df: Input LazyFrame with parental data
            results: List to append results to
            n_total: Total number of records
            parent_type: 'Father' or 'Mother'
            stratify_by: Column name to stratify by (optional)
            study_years: List of years to analyze
        """
        prefix = "FAR" if parent_type == "Father" else "MOR"

        # Calculate age at child's birth with null handling
        df_with_age = df.with_columns(
            [
                pl.when(pl.col("FOED_DAG").is_not_null() & pl.col(f"{prefix}_FDAG").is_not_null())
                .then(
                    (pl.col("FOED_DAG").cast(pl.Date) - pl.col(f"{prefix}_FDAG").cast(pl.Date)).dt.total_days() / 365.25
                )
                .otherwise(None)
                .alias(f"{prefix}_AGE_AT_BIRTH")
            ]
        )

        # Select all numeric columns that start with the parent prefix
        parent_numeric_cols = [
            col
            for col in df_with_age.collect_schema().keys()
            if col.startswith(prefix)
            and isinstance(
                df_with_age.collect_schema()[col], pl.Int8 | pl.Int16 | pl.Int32 | pl.Int64 | pl.Float32 | pl.Float64
            )
        ]

        # Select all categorical/string columns that start with the parent prefix
        parent_cat_cols = [
            col
            for col in df_with_age.collect_schema().keys()
            if col.startswith(prefix) and isinstance(df_with_age.collect_schema()[col], pl.Categorical | pl.Utf8)
        ]

        for year in study_years:
            try:
                # Filter data for the specific year
                year_data = df_with_age.filter(pl.col("year") == year)

                # Calculate statistics for numeric columns
                for col in parent_numeric_cols:
                    try:
                        stats = self.calculate_stats(year_data, col, stratify_by)
                        for stratum, stat in stats.items():
                            results.append(
                                {
                                    "Category": f"{parent_type} Characteristics",
                                    "Variable": f"{col} {year}{f' - {stratum}' if stratum != 'all' else ''}",
                                    "Value": (
                                        f"Mean: {self.format_number(stat.get('mean', 0))} "
                                        f"(SD: {self.format_number(stat.get('std', 0))})\n"
                                        f"Median: {self.format_number(stat.get('median', 0))} "
                                        f"(IQR: {self.format_number(stat.get('q1', 0))}-"
                                        f"{self.format_number(stat.get('q3', 0))})"
                                    ),
                                    "Missing": f"{(stat.get('missing', 0)/n_total*100):.1f}%",
                                }
                            )
                    except Exception as e:
                        logger.warning(f"Error processing {col} {year}: {str(e)}")
                        continue

                # Handle categorical variables
                for col in parent_cat_cols:
                    try:
                        cat_stats = (
                            year_data.group_by(col)
                            .agg(pl.count().alias("count"))
                            .with_columns((pl.col("count") / pl.lit(n_total) * 100).alias("percentage"))
                            .sort("count", descending=True)
                            .collect()
                        )

                        if len(cat_stats) > 0:
                            distribution = "\n".join(
                                f"{str(val)}: {count:,} ({pct:.1f}%)"
                                for val, count, pct in zip(
                                    cat_stats[col].to_list(),
                                    cat_stats["count"].to_list(),
                                    cat_stats["percentage"].to_list(),
                                    strict=False,
                                )
                                if val is not None
                            )

                            missing_count = year_data.filter(pl.col(col).is_null()).select(pl.count()).collect().item()

                            results.append(
                                {
                                    "Category": f"{parent_type} Characteristics",
                                    "Variable": f"{col} {year}",
                                    "Value": distribution,
                                    "Missing": f"{(missing_count/n_total*100):.1f}%",
                                }
                            )

                    except Exception as e:
                        logger.warning(f"Error processing categorical variable {col} {year}: {str(e)}")
                        continue

                # Add age at birth specifically
                try:
                    age_stats = self.calculate_stats(year_data, f"{prefix}_AGE_AT_BIRTH", stratify_by)

                    for stratum, stat in age_stats.items():
                        if all(k in stat for k in ["mean", "std", "median", "q1", "q3"]):
                            results.append(
                                {
                                    "Category": f"{parent_type} Characteristics",
                                    "Variable": f"Age at birth {year}{f' - {stratum}' if stratum != 'all' else ''}",
                                    "Value": (
                                        f"Mean: {self.format_number(stat['mean'])} "
                                        f"(SD: {self.format_number(stat['std'])})\n"
                                        f"Median: {self.format_number(stat['median'])} "
                                        f"(IQR: {self.format_number(stat['q1'])}-"
                                        f"{self.format_number(stat['q3'])})"
                                    ),
                                    "Missing": f"{(stat.get('missing', 0)/n_total*100):.1f}%",
                                }
                            )
                except Exception as e:
                    logger.warning(f"Error processing age at birth for {parent_type} {year}: {str(e)}")

            except Exception as e:
                logger.error(f"Error processing {parent_type} characteristics for year {year}: {str(e)}")
                continue

        # Log completion
        logger.debug(f"Completed processing {parent_type} characteristics")
