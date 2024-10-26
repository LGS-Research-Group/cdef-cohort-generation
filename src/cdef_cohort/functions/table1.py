from datetime import datetime
from pathlib import Path
from typing import Any, TypedDict

import pandas as pd
import polars as pl


class TableData(TypedDict):
    latex: Any  # Could be str or pd.DataFrame
    html: Any  # Could be str or pd.DataFrame
    excel: pd.DataFrame
    csv: pd.DataFrame


TablesDict = dict[str, str | pd.DataFrame]


def format_number(value: float, decimals: int = 1) -> str:
    """Format numbers with thousands separator and specified decimals."""
    return f"{value:,.{decimals}f}"


def calculate_stats(df: pl.LazyFrame, column: str, strata_name: str | None = None) -> dict[str, dict[str, float]]:
    """Calculate summary statistics for a given column."""
    if strata_name:
        stats = (
            df.filter(pl.col(strata_name).is_not_null())
            .group_by(strata_name)
            .agg(
                [
                    pl.col(column).mean().alias("mean"),
                    pl.col(column).std().alias("std"),
                    pl.col(column).median().alias("median"),
                    pl.col(column).quantile(0.25).alias("q1"),
                    pl.col(column).quantile(0.75).alias("q3"),
                    pl.col(column).count().alias("count"),
                    pl.col(column).is_null().sum().alias("missing"),
                ]
            )
            .collect()
        )
        return {
            str(row[0]): {
                "mean": row[1],
                "std": row[2],
                "median": row[3],
                "q1": row[4],
                "q3": row[5],
                "n": row[6],
                "missing": row[7],
            }
            for row in stats.iter_rows()
        }
    else:
        stats = df.select(
            [
                pl.col(column).mean().alias("mean"),
                pl.col(column).std().alias("std"),
                pl.col(column).median().alias("median"),
                pl.col(column).quantile(0.25).alias("q1"),
                pl.col(column).quantile(0.75).alias("q3"),
                pl.col(column).count().alias("count"),
                pl.col(column).is_null().sum().alias("missing"),
            ]
        ).collect()
        return {
            "all": {
                "mean": stats["mean"][0],
                "std": stats["std"][0],
                "median": stats["median"][0],
                "q1": stats["q1"][0],
                "q3": stats["q3"][0],
                "n": stats["count"][0],
                "missing": stats["missing"][0],
            }
        }


def create_stratified_table_one(
    df: pl.LazyFrame,
    study_years: list[int] | None = None,
    stratify_by: str | None = None,
    output_formats: list[str] | None = None,
) -> TablesDict:
    """
    Create comprehensive Table 1 with optional stratification.

    Args:
        df: Main dataframe
        study_years: Years to analyze for longitudinal data (default: [2005, 2010, 2015, 2020])
        stratify_by: Column to stratify by (e.g., "KOEN" for sex)
        output_formats: List of desired output formats (default: ["latex", "html", "excel", "csv"])
    """
    # Use textwrap.dedent() for multiline strings

    if study_years is None:
        study_years = [2005, 2010, 2015, 2020]
    if output_formats is None:
        output_formats = ["latex", "html", "excel", "csv"]

    results = []
    n_total = df.select(pl.n_unique("PNR")).collect().item()

    # CHILD CHARACTERISTICS
    results.append(
        {
            "Category": "Child Characteristics",
            "Variable": "Total children",
            "Overall": f"{n_total:,}",
            "Missing": "0%",
        }
    )

    # Child age
    child_age_stats = calculate_stats(df, "FOED_DAG", stratify_by)
    for stratum, stats in child_age_stats.items():
        results.append(
            {
                "Category": "Child Characteristics",
                "Variable": f"Age at inclusion (years){f' - {stratum}' if stratum != 'all' else ''}",
                "Value": f"Mean: {format_number(stats['mean'])} (SD: {format_number(stats['std'])})\n"
                + f"Median: {format_number(stats['median'])} "
                + f"(IQR: {format_number(stats['q1'])}-{format_number(stats['q3'])})",
                "Missing": f"{(stats['missing']/n_total*100):.1f}%",
            }
        )

    # Sex distribution
    (
        df.group_by("KOEN")
        .agg(pl.count().alias("count"))
        .with_columns((pl.col("count") / n_total * 100).alias("percentage"))
        .collect()
    )

    # MATERNAL CHARACTERISTICS
    # Mother's age
    mother_age_stats = calculate_stats(df, "MOR_FDAG", stratify_by)
    for stratum, stats in mother_age_stats.items():
        results.append(
            {
                "Category": "Maternal Characteristics",
                "Variable": f"Mother's age at child birth{f' - {stratum}' if stratum != 'all' else ''}",
                "Value": f"Mean: {format_number(stats['mean'])} (SD: {format_number(stats['std'])})\n"
                + f"Median: {format_number(stats['median'])} "
                + f"(IQR: {format_number(stats['q1'])}-{format_number(stats['q3'])})",
                "Missing": f"{(stats['missing']/n_total*100):.1f}%",
            }
        )

    # Mother's education
    for year in study_years:
        (
            df.filter(pl.col("year") == year)
            .group_by("MOR_EDU_LVL")
            .agg(pl.count().alias("count"))
            .with_columns((pl.col("count") / n_total * 100).alias("percentage"))
            .sort("count", descending=True)
            .collect()
        )
        results.append(
            {
                "Category": "Maternal Characteristics",
                "Variable": f"Mother's education {year}",
                "Value": "Distribution",
                "Missing": f"{(df.filter(pl.col('year') == year).select(
                    pl.col('MOR_EDU_LVL').is_null().sum()).collect().item() / n_total * 100):.1f}%",
            }
        )

    # Mother's income and employment
    for year in study_years:
        mother_income_stats = calculate_stats(df.filter(pl.col("year") == year), "MOR_PERINDKIALT_13", stratify_by)

        for stratum, stats in mother_income_stats.items():
            results.append(
                {
                    "Category": "Maternal Characteristics",
                    "Variable": f"Mother's income {year} (DKK){f' - {stratum}' if stratum != 'all' else ''}",
                    "Value": f"Mean: {format_number(stats['mean'], 0)} (SD: {format_number(stats['std'], 0)})\n"
                    + f"Median: {format_number(stats['median'], 0)} "
                    + f"(IQR: {format_number(stats['q1'], 0)}-{format_number(stats['q3'], 0)})",
                    "Missing": f"{(stats['missing']/n_total*100):.1f}%",
                }
            )

        # Mother's employment status
        (
            df.filter(pl.col("year") == year)
            .group_by("MOR_BESKST13")
            .agg(pl.count().alias("count"))
            .with_columns((pl.col("count") / n_total * 100).alias("percentage"))
            .sort("count", descending=True)
            .collect()
        )

        results.append(
            {
                "Category": "Maternal Characteristics",
                "Variable": f"Mother's employment status {year}",
                "Value": "Distribution",
                "Missing": f"{(df.filter(pl.col('year') == year).select(
                pl.col('MOR_BESKST13').is_null().sum()).collect().item() / n_total * 100):.1f}%",
            }
        )

    # PATERNAL CHARACTERISTICS
    # Father's age
    father_age_stats = calculate_stats(df, "FAR_FDAG", stratify_by)
    for stratum, stats in father_age_stats.items():
        results.append(
            {
                "Category": "Paternal Characteristics",
                "Variable": f"Father's age at child birth{f' - {stratum}' if stratum != 'all' else ''}",
                "Value": f"Mean: {format_number(stats['mean'])} (SD: {format_number(stats['std'])})\n"
                + f"Median: {format_number(stats['median'])} "
                + f"(IQR: {format_number(stats['q1'])}-{format_number(stats['q3'])})",
                "Missing": f"{(stats['missing']/n_total*100):.1f}%",
            }
        )

    # Father's education
    for year in study_years:
        (
            df.filter(pl.col("year") == year)
            .group_by("FAR_EDU_LVL")
            .agg(pl.count().alias("count"))
            .with_columns((pl.col("count") / n_total * 100).alias("percentage"))
            .sort("count", descending=True)
            .collect()
        )
        results.append(
            {
                "Category": "Paternal Characteristics",
                "Variable": f"Father's education {year}",
                "Value": "Distribution",
                "Missing": f"{(df.filter(pl.col('year') == year).select(
                    pl.col('FAR_EDU_LVL').is_null().sum()).collect().item() / n_total * 100):.1f}%",
            }
        )

    # Father's income and employment
    for year in study_years:
        father_income_stats = calculate_stats(df.filter(pl.col("year") == year), "FAR_PERINDKIALT_13", stratify_by)

        for stratum, stats in father_income_stats.items():
            results.append(
                {
                    "Category": "Paternal Characteristics",
                    "Variable": f"Father's income {year} (DKK){f' - {stratum}' if stratum != 'all' else ''}",
                    "Value": f"Mean: {format_number(stats['mean'], 0)} (SD: {format_number(stats['std'], 0)})\n"
                    + f"Median: {format_number(stats['median'], 0)} "
                    + f"(IQR: {format_number(stats['q1'], 0)}-{format_number(stats['q3'], 0)})",
                    "Missing": f"{(stats['missing']/n_total*100):.1f}%",
                }
            )

        # Father's employment status
        (
            df.filter(pl.col("year") == year)
            .group_by("FAR_BESKST13")
            .agg(pl.count().alias("count"))
            .with_columns((pl.col("count") / n_total * 100).alias("percentage"))
            .sort("count", descending=True)
            .collect()
        )

        results.append(
            {
                "Category": "Paternal Characteristics",
                "Variable": f"Father's employment status {year}",
                "Value": "Distribution",
                "Missing": f"{(df.filter(pl.col('year') == year).select(
                pl.col('FAR_BESKST13').is_null().sum()).collect().item() / n_total * 100):.1f}%",
            }
        )

    # FAMILY CHARACTERISTICS
    (
        df.group_by("FAMILIE_TYPE")
        .agg(pl.count().alias("count"))
        .with_columns((pl.col("count") / n_total * 100).alias("percentage"))
        .sort("count", descending=True)
        .collect()
    )

    calculate_stats(df, "ANTPERSH", stratify_by)
    calculate_stats(df, "ANTBOERNF", stratify_by)

    # Convert to pandas DataFrame
    table_df = pd.DataFrame(results)

    # Create styled version for each format
    tables = {}

    if "latex" in output_formats:
        tables["latex"] = create_latex_table(table_df)
    if "html" in output_formats:
        tables["html"] = create_html_table(table_df)
    if "excel" in output_formats:
        tables["excel"] = table_df
    if "csv" in output_formats:
        tables["csv"] = table_df

    return tables


def create_latex_table(df: pd.DataFrame) -> str:
    """Convert DataFrame to LaTeX format."""
    return df.to_latex(index=False, escape=False)


def create_html_table(df: pd.DataFrame) -> str:
    """Convert DataFrame to HTML format."""
    return df.to_html(index=False)


def save_tables(tables: TablesDict, output_dir: Path, prefix: str = "table_one") -> None:
    """Save tables in specified formats."""
    timestamp = datetime.now().strftime("%Y%m%d")
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except PermissionError as e:
        raise RuntimeError(f"Cannot create output directory: {output_dir}") from e

    for format_type, table in tables.items():
        try:
            output_path = output_dir / f"{prefix}_{timestamp}.{format_type}"
            if format_type in ("latex", "html"):
                content = (
                    (create_latex_table(table) if format_type == "latex" else create_html_table(table))
                    if isinstance(table, pd.DataFrame)
                    else str(table)
                )
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(content)
            elif format_type == "excel" and isinstance(table, pd.DataFrame):
                table.to_excel(output_path, index=False)
            elif format_type == "csv" and isinstance(table, pd.DataFrame):
                table.to_csv(output_path, index=False)
            else:
                raise ValueError(f"Invalid format type or table format: {format_type}")
        except Exception as e:
            raise RuntimeError(f"Error saving {format_type} table: {str(e)}") from e


def main() -> None:
    """
    Main function to demonstrate table creation and saving functionality.

    This function provides example usage of the table creation and saving functions
    with both unstratified and sex-stratified tables.
    """
    # Example usage
    output_dir = Path("tables")
    df = pl.scan_parquet("path/to/your/data.parquet")

    # Create unstratified table
    tables = create_stratified_table_one(df)
    save_tables(tables, output_dir)

    # Create table stratified by sex
    stratified_tables = create_stratified_table_one(df, stratify_by="KOEN")
    save_tables(stratified_tables, output_dir, prefix="table_one_stratified_by_sex")


if __name__ == "__main__":
    main()
