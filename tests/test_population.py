from datetime import date

import polars as pl
import pytest
from polars.exceptions import ColumnNotFoundError

from cdef_cohort.population import main as process_population


@pytest.fixture
def mock_bef_data():
    """Create mock BEF data for testing."""
    return pl.DataFrame(
        {
            "PNR": ["1", "2", "3", "4", "5"],
            "FAR_ID": ["F1", "F2", "F2", "F4", "F5"],
            "MOR_ID": ["M1", "M2", "M2", "M4", "M5"],
            "FAMILIE_ID": ["FAM1", "FAM2", "FAM2", "FAM4", "FAM5"],
            "FOED_DAG": [
                "2010-01-01",
                "2011-02-02",
                "2011-02-02",  # Duplicate to test unique filtering
                "2012-03-03",
                "2013-04-04",
            ],
        }
    )


@pytest.fixture
def mock_mfr_data():
    """Create mock MFR data for testing."""
    return pl.DataFrame(
        {
            "CPR_BARN": ["3", "4", "5", "6", "7"],  # Some overlap, some unique
            "CPR_FADER": ["F3", "F4", "F5", "F6", "F7"],
            "CPR_MODER": ["M3", "M4", "M5", "M6", "M7"],
            "FOEDSELSDATO": [
                "2011-02-02",
                "2012-03-03",
                "2013-04-04",
                "2012-05-05",
                "2013-06-06",
            ],
        }
    )


@pytest.fixture
def temp_dir(tmp_path):
    """Create temporary directory structure."""
    bef_dir = tmp_path / "bef"
    mfr_dir = tmp_path / "mfr"
    output_dir = tmp_path / "output"
    for dir_path in [bef_dir, mfr_dir, output_dir]:
        dir_path.mkdir()
    return tmp_path


@pytest.fixture
def setup_test_files(temp_dir, mock_bef_data, mock_mfr_data, monkeypatch):
    """Setup test files and modify configuration paths."""
    # Save mock data
    bef_file = temp_dir / "bef" / "test_bef.parquet"
    mfr_file = temp_dir / "mfr" / "test_mfr.parquet"
    mock_bef_data.write_parquet(bef_file)
    mock_mfr_data.write_parquet(mfr_file)

    # Setup output path
    output_file = temp_dir / "output" / "population.parquet"

    # Patch configuration paths
    monkeypatch.setattr("cdef_cohort.population.BEF_FILES", bef_file)
    monkeypatch.setattr("cdef_cohort.population.MFR_FILES", mfr_file)
    monkeypatch.setattr("cdef_cohort.population.POPULATION_FILE", output_file)
    monkeypatch.setattr("cdef_cohort.population.BIRTH_INCLUSION_START_YEAR", 2010)
    monkeypatch.setattr("cdef_cohort.population.BIRTH_INCLUSION_END_YEAR", 2013)

    return {
        "bef_file": bef_file,
        "mfr_file": mfr_file,
        "output_file": output_file,
        "output_dir": temp_dir / "output",
    }


def test_population_processing(setup_test_files):
    """Test the main population processing function."""
    process_population()

    result = pl.read_parquet(setup_test_files["output_file"])
    summary_before = pl.read_parquet(
        setup_test_files["output_dir"] / "population_summary_before.parquet"
    )
    summary_after = pl.read_parquet(
        setup_test_files["output_dir"] / "population_summary_after.parquet"
    )

    # Check basic structure
    assert len(result) == 7  # Combined unique records from both sources
    assert all(
        col in result.columns
        for col in ["PNR", "FOED_DAG", "FAR_ID", "FAR_FDAG", "MOR_ID", "MOR_FDAG", "FAMILIE_ID"]
    )

    # Check data types
    schema = result.schema
    assert schema["PNR"] == pl.Utf8
    assert schema["FAR_ID"] == pl.Utf8
    assert schema["MOR_ID"] == pl.Utf8
    assert schema["FAMILIE_ID"] == pl.Utf8
    assert isinstance(result["FOED_DAG"][0], date)

    # Check summaries
    assert summary_before["total_bef_records"][0] == 5
    assert summary_before["total_mfr_records"][0] == 5
    assert summary_after["total_combined_records"][0] == 7


def test_data_source_priority(setup_test_files):
    """Test that BEF data takes priority over MFR data when both exist."""
    process_population()
    result = pl.read_parquet(setup_test_files["output_file"])

    # Check overlapping records (3, 4, 5)
    overlapping = result.filter(pl.col("PNR").is_in(["3", "4", "5"]))
    assert all(overlapping["FAMILIE_ID"].is_not_null())  # Should have BEF FAMILIE_ID


def test_mfr_only_records(setup_test_files):
    """Test handling of records that only exist in MFR."""
    process_population()
    result = pl.read_parquet(setup_test_files["output_file"])

    # Check MFR-only records (6, 7)
    mfr_only = result.filter(pl.col("PNR").is_in(["6", "7"]))
    assert all(mfr_only["FAMILIE_ID"].is_null())  # Should have null FAMILIE_ID


@pytest.mark.parametrize(
    "data_source,file_key,required_columns",
    [
        ("BEF", "bef_file", ["PNR", "FAR_ID", "MOR_ID", "FAMILIE_ID", "FOED_DAG"]),
        ("MFR", "mfr_file", ["CPR_BARN", "CPR_FADER", "CPR_MODER", "FOEDSELSDATO"]),
    ],
)
def test_missing_columns(
    setup_test_files, mock_bef_data, mock_mfr_data, data_source, file_key, required_columns
):
    """Test handling of missing columns in input data."""
    for col in required_columns:
        # Create data with missing column
        if data_source == "BEF":
            reduced_data = mock_bef_data.drop(col)
        else:
            reduced_data = mock_mfr_data.drop(col)

        reduced_data.write_parquet(setup_test_files[file_key])

        with pytest.raises(ColumnNotFoundError):
            process_population()


def test_empty_input(setup_test_files, mock_bef_data, mock_mfr_data):
    """Test handling of empty input data."""
    # Create empty data
    mock_bef_data.head(0).write_parquet(setup_test_files["bef_file"])
    mock_mfr_data.head(0).write_parquet(setup_test_files["mfr_file"])

    process_population()

    result = pl.read_parquet(setup_test_files["output_file"])
    assert len(result) == 0


def test_summary_statistics(setup_test_files):
    """Test that summary statistics are correctly generated."""
    process_population()

    summary_before = pl.read_parquet(
        setup_test_files["output_dir"] / "population_summary_before.parquet"
    )
    summary_after = pl.read_parquet(
        setup_test_files["output_dir"] / "population_summary_after.parquet"
    )

    # Check summary content
    assert "total_bef_records" in summary_before.columns
    assert "total_mfr_records" in summary_before.columns
    assert "total_combined_records" in summary_after.columns
    assert "records_only_in_bef" in summary_after.columns
    assert "records_only_in_mfr" in summary_after.columns
