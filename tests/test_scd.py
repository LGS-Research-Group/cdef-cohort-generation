from datetime import date

import polars as pl
import pytest

from cdef_cohort.main import apply_scd_algorithm, identify_severe_chronic_disease, process_lpr_data


@pytest.fixture
def mock_lpr2_data():
    """Create mock LPR2 data."""
    adm_data = pl.DataFrame(
        {
            "PNR": ["1", "2", "3"],
            "C_ADIAG": ["DE750", "DZ000", "DE760"],  # E75.0 is an SCD code
            "RECNUM": ["R1", "R2", "R3"],
            "D_INDDTO": ["2010-01-01", "2010-01-02", "2010-01-03"],
        }
    ).with_columns(
        [
            pl.col("PNR").cast(pl.Utf8),
            pl.col("C_ADIAG").cast(pl.Utf8),
            pl.col("RECNUM").cast(pl.Utf8),
            pl.col("D_INDDTO").cast(pl.Date),
        ]
    )

    diag_data = pl.DataFrame(
        {
            "RECNUM": ["R1", "R2", "R3"],
            "C_DIAG": ["DE750", "DZ000", "DE760"],
            "C_TILDIAG": ["DZ000", "DZ000", "DE750"],
        }
    ).with_columns(
        [
            pl.col("RECNUM").cast(pl.Utf8),
            pl.col("C_DIAG").cast(pl.Utf8),
            pl.col("C_TILDIAG").cast(pl.Utf8),
        ]
    )

    bes_data = pl.DataFrame(
        {"RECNUM": ["R1", "R2", "R3"], "D_AMBDTO": ["2010-01-01", "2010-01-02", "2010-01-03"]}
    ).with_columns([pl.col("RECNUM").cast(pl.Utf8), pl.col("D_AMBDTO").cast(pl.Date)])

    return {"adm": adm_data.lazy(), "diag": diag_data.lazy(), "bes": bes_data.lazy()}


@pytest.fixture
def mock_lpr3_data():
    """Create mock LPR3 data."""
    kontakter_data = pl.DataFrame(
        {
            "DW_EK_KONTAKT": ["K1", "K2", "K3"],
            "CPR": ["4", "5", "6"],
            "aktionsdiagnose": ["DE840", "DZ000", "DE840"],  # E84.0 is an SCD code
            "dato_start": ["2020-01-01", "2020-01-02", "2020-01-03"],
        }
    ).with_columns(
        [
            pl.col("DW_EK_KONTAKT").cast(pl.Utf8),
            pl.col("CPR").cast(pl.Utf8),
            pl.col("aktionsdiagnose").cast(pl.Utf8),
            pl.col("dato_start").cast(pl.Date),
        ]
    )

    diagnoser_data = pl.DataFrame(
        {"DW_EK_KONTAKT": ["K1", "K2", "K3"], "diagnosekode": ["DE840", "DZ000", "DE760"]}
    ).with_columns([pl.col("DW_EK_KONTAKT").cast(pl.Utf8), pl.col("diagnosekode").cast(pl.Utf8)])

    return {"kontakter": kontakter_data.lazy(), "diagnoser": diagnoser_data.lazy()}


@pytest.fixture
def mock_files(tmp_path, mock_lpr2_data, mock_lpr3_data, monkeypatch):
    """Setup mock files and paths."""
    # Create all required directories
    lpr2_adm_dir = tmp_path / "registers" / "lpr_adm"
    lpr2_adm_dir.mkdir(parents=True)

    # Create a sample file in the directory
    lpr2_adm_file = lpr2_adm_dir / "sample.parquet"
    mock_lpr2_data["adm"].collect().write_parquet(lpr2_adm_file)

    # Create and write other files
    lpr2_diag_file = tmp_path / "registers" / "lpr_diag" / "sample.parquet"
    lpr2_diag_file.parent.mkdir(parents=True, exist_ok=True)
    mock_lpr2_data["diag"].collect().write_parquet(lpr2_diag_file)

    lpr2_bes_file = tmp_path / "registers" / "lpr_bes" / "sample.parquet"
    lpr2_bes_file.parent.mkdir(parents=True, exist_ok=True)
    mock_lpr2_data["bes"].collect().write_parquet(lpr2_bes_file)

    lpr3_kontakter_file = tmp_path / "registers" / "lpr3_kontakter" / "sample.parquet"
    lpr3_kontakter_file.parent.mkdir(parents=True, exist_ok=True)
    mock_lpr3_data["kontakter"].collect().write_parquet(lpr3_kontakter_file)

    lpr3_diagnoser_file = tmp_path / "registers" / "lpr3_diagnoser" / "sample.parquet"
    lpr3_diagnoser_file.parent.mkdir(parents=True, exist_ok=True)
    mock_lpr3_data["diagnoser"].collect().write_parquet(lpr3_diagnoser_file)

    # Patch configuration paths to use directories instead of specific files
    monkeypatch.setattr("cdef_cohort.utils.config.LPR_ADM_OUT", lpr2_adm_dir)
    monkeypatch.setattr("cdef_cohort.utils.config.LPR_DIAG_OUT", lpr2_diag_file.parent)
    monkeypatch.setattr("cdef_cohort.utils.config.LPR_BES_OUT", lpr2_bes_file.parent)
    monkeypatch.setattr("cdef_cohort.utils.config.LPR3_KONTAKTER_OUT", lpr3_kontakter_file.parent)
    monkeypatch.setattr("cdef_cohort.utils.config.LPR3_DIAGNOSER_OUT", lpr3_diagnoser_file.parent)

    return tmp_path


def test_process_lpr_data(mock_files):
    """Test processing of LPR data."""
    lpr2, lpr3 = process_lpr_data()

    # Verify LPR2 data
    lpr2_result = lpr2.collect()
    assert len(lpr2_result) == 3
    assert all(col in lpr2_result.columns for col in ["PNR", "C_ADIAG", "RECNUM", "D_INDDTO"])

    # Verify LPR3 data
    lpr3_result = lpr3.collect()
    assert len(lpr3_result) == 3
    assert all(col in lpr3_result.columns for col in ["CPR", "aktionsdiagnose", "dato_start"])


def test_apply_scd_algorithm():
    """Test SCD algorithm application."""
    # Update test data to match expected SCD codes
    test_data = (
        pl.DataFrame(
            {
                "id": ["1", "2", "3"],
                "diag": ["DE840", "DZ000", "DE840"],  # E84.0 is definitely an SCD code
                "date": ["2020-01-01", "2020-01-02", "2020-01-03"],
            }
        )
        .with_columns(
            [pl.col("id").cast(pl.Utf8), pl.col("diag").cast(pl.Utf8), pl.col("date").cast(pl.Date)]
        )
        .lazy()
    )

    result = apply_scd_algorithm(
        test_data, diagnosis_cols=["diag"], date_col="date", id_col="id"
    ).collect()

    assert len(result) == 3
    # E84.0 is an SCD code, so both records with this code should be True
    assert result["is_scd"].to_list() == [True, False, True]
    assert result["first_scd_date"][0] == date(2020, 1, 1)


def test_identify_severe_chronic_disease(mock_files):
    """Test the complete SCD identification process."""
    scd_data = identify_severe_chronic_disease()
    result = scd_data.collect()

    # Basic checks
    assert len(result) > 0
    assert all(col in result.columns for col in ["PNR", "is_scd", "first_scd_date"])

    # Verify SCD identification (at least one case should be identified)
    scd_cases = result.filter(pl.col("is_scd"))
    assert len(scd_cases) > 0

    # Check date formatting
    dates = result["first_scd_date"].drop_nulls()
    assert all(isinstance(d, date) for d in dates)


def test_scd_edge_cases():
    """Test edge cases in SCD identification."""
    # Test with empty data but valid schema
    empty_df = (
        pl.DataFrame({"PNR": [], "C_ADIAG": [], "RECNUM": [], "D_INDDTO": []})
        .with_columns(
            [
                pl.col("PNR").cast(pl.Utf8),
                pl.col("C_ADIAG").cast(pl.Utf8),
                pl.col("RECNUM").cast(pl.Utf8),
                pl.col("D_INDDTO").cast(pl.Date),
            ]
        )
        .lazy()
    )

    result = apply_scd_algorithm(
        empty_df, diagnosis_cols=["C_ADIAG"], date_col="D_INDDTO", id_col="PNR"
    ).collect()

    assert len(result) == 0


def test_scd_invalid_codes():
    """Test handling of invalid diagnosis codes."""
    invalid_data = (
        pl.DataFrame(
            {
                "id": ["1", "2"],
                "diag": ["INVALID", "NOT_A_CODE"],
                "date": ["2020-01-01", "2020-01-02"],
            }
        )
        .with_columns(
            [pl.col("id").cast(pl.Utf8), pl.col("diag").cast(pl.Utf8), pl.col("date").cast(pl.Date)]
        )
        .lazy()
    )

    result = apply_scd_algorithm(
        invalid_data, diagnosis_cols=["diag"], date_col="date", id_col="id"
    ).collect()

    assert len(result) == 2
    assert not any(result["is_scd"])
    assert all(date is None for date in result["first_scd_date"])
