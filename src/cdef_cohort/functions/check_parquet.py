from typing import Any

import polars as pl
import polars.selectors as cs


def analyze_parquet(file_path: str) -> dict[str, Any]:
    # Load the Parquet file
    df = pl.scan_parquet(file_path)

    # Get basic information
    num_rows = df.select(pl.count()).collect().item()
    num_cols = len(df.columns)

    # Compute statistics for all columns in a single pass
    stats = df.select(
        [
            pl.all().null_count().alias("null_count"),
            pl.all().n_unique().alias("n_unique"),
            pl.all().min().alias("min"),
            pl.all().max().alias("max"),
            pl.all().mean().alias("mean"),
            pl.all().median().alias("median"),
        ]
    ).collect()

    # Get data types
    dtypes = df.schema

    # Get numeric columns
    numeric_cols = df.select(cs.numeric()).columns

    # Compile column statistics
    column_stats = []
    for col in df.columns:
        col_stats = {
            "column": col,
            "dtype": str(dtypes[col]),
            "num_missing": stats.get_column("null_count")[df.columns.index(col)],
            "num_unique": stats.get_column("n_unique")[df.columns.index(col)],
            "min": stats.get_column("min")[df.columns.index(col)],
            "max": stats.get_column("max")[df.columns.index(col)],
            "mean": stats.get_column("mean")[df.columns.index(col)],
            "median": stats.get_column("median")[df.columns.index(col)],
        }

        # Set numeric stats to None for non-numeric columns
        if col not in numeric_cols:
            col_stats.update({k: None for k in ["min", "max", "mean", "median"]})

        column_stats.append(col_stats)

    # Create a summary dictionary
    summary = {
        "file_path": file_path,
        "num_rows": num_rows,
        "num_columns": num_cols,
        "column_stats": column_stats,
    }

    return summary

if __name__ == "__main__":
    # Example usage
    file_path = "path/to/your/file.parquet"
    result = analyze_parquet(file_path)

    # Print the results
    print(f"File: {result['file_path']}")
    print(f"Number of rows: {result['num_rows']}")
    print(f"Number of columns: {result['num_columns']}")
    print("\nColumn Statistics:")
    for col_stat in result["column_stats"]:
        print(f"\nColumn: {col_stat['column']}")
        print(f"  Data Type: {col_stat['dtype']}")
        print(f"  Missing Values: {col_stat['num_missing']}")
        print(f"  Unique Values: {col_stat['num_unique']}")
        if col_stat["min"] is not None:
            print(f"  Min: {col_stat['min']}")
            print(f"  Max: {col_stat['max']}")
            print(f"  Mean: {col_stat['mean']}")
            print(f"  Median: {col_stat['median']}")
