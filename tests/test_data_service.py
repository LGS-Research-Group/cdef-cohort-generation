from pathlib import Path

import polars as pl

from cdef_cohort.services.data_service import DataService


def test_data_service_read_write(test_data_dir: Path):
    # Arrange
    service = DataService()
    test_df = pl.DataFrame({
        "col1": ["a", "b", "c"],
        "col2": [1, 2, 3]
    }).lazy()

    test_file = test_data_dir / "test.parquet"

    # Act
    service.write_parquet(test_df, test_file)
    result = service.read_parquet(test_file)

    # Assert
    assert result.collect_schema() == test_df.collect_schema()
    assert result.collect().shape == test_df.collect().shape

def test_data_service_schema_validation():
    # Arrange
    service = DataService()
    test_df = pl.DataFrame({
        "col1": ["a", "b", "c"],
        "col2": [1, 2, 3]
    }).lazy()

    expected_schema = {
        "col1": pl.Utf8,
        "col2": pl.Int64
    }

    # Act
    is_valid = service.validate_schema(test_df, expected_schema)

    # Assert
    assert is_valid
