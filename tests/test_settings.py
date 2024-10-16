import os
import tempfile
from pathlib import Path

import pytest
from pydantic import ValidationError

from cdef_cohort_builder.settings import Settings, SubModel


@pytest.fixture
def mock_env(monkeypatch):
    monkeypatch.setenv("CDEF_BASE_DIR", "/tmp/test_dir")
    monkeypatch.setenv("CDEF_BIRTH_INCLUSION_START_YEAR", "2000")
    monkeypatch.setenv("CDEF_BIRTH_INCLUSION_END_YEAR", "2010")
    monkeypatch.setenv("CDEF_SUB_MODEL__V1", "test_v1")
    monkeypatch.setenv("CDEF_SUB_MODEL__V2", "test_v2")
    monkeypatch.setenv("CDEF_SUB_MODEL__V3", "3")
    monkeypatch.setenv("CDEF_SUB_MODEL__V4", "test_v4")


def test_default_settings():
    settings = Settings()
    assert settings.BASE_DIR == Path("/Users/tobiaskragholm/dev/TEST_RUN")
    assert settings.DATA_DIR == settings.BASE_DIR / "data"
    assert settings.REGISTER_DIR == settings.BASE_DIR / "registers"
    assert settings.BIRTH_INCLUSION_START_YEAR == 1995
    assert settings.BIRTH_INCLUSION_END_YEAR == 2020


def test_env_override(mock_env):
    settings = Settings()
    assert settings.BASE_DIR == Path("/tmp/test_dir")
    assert settings.DATA_DIR == Path("/tmp/test_dir/data")
    assert settings.REGISTER_DIR == Path("/tmp/test_dir/registers")
    assert settings.BIRTH_INCLUSION_START_YEAR == 2000
    assert settings.BIRTH_INCLUSION_END_YEAR == 2010
    assert settings.sub_model.v1 == "test_v1"
    assert settings.sub_model.v2 == b"test_v2"
    assert settings.sub_model.v3 == 3
    assert settings.sub_model.v4 == "test_v4"


def test_path_validator():
    settings = Settings(BASE_DIR=Path("/some/custom/path"))
    assert isinstance(settings.BASE_DIR, Path)
    assert settings.BASE_DIR == Path("/some/custom/path")
    assert settings.DATA_DIR == Path("/some/custom/path/data")


def test_invalid_year_setting():
    with pytest.raises(ValidationError):
        Settings(BIRTH_INCLUSION_START_YEAR=1800)


def test_derived_paths():
    settings = Settings(BASE_DIR=Path("/custom/base"))
    assert settings.POPULATION_FILE == Path("/custom/base/data/population.parquet")
    assert settings.BEF_FILES == Path("/custom/base/registers/bef/*.parquet")


def test_env_file(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("CDEF_BASE_DIR=/env/file/path\nCDEF_BIRTH_INCLUSION_START_YEAR=2005\n")

    os.environ["ENV_FILE"] = str(env_file)
    settings = Settings()
    del os.environ["ENV_FILE"]

    assert settings.BASE_DIR == Path("/env/file/path")
    assert settings.BIRTH_INCLUSION_START_YEAR == 2005


def test_settings_immutability():
    settings = Settings()
    with pytest.raises(TypeError):  # Pydantic v2 raises TypeError for frozen models
        settings.BASE_DIR = Path("/new/path")


def test_cli_subcommand():
    settings = Settings(sub_model=SubModel(v1="cli_v1", v2=b"cli_v2", v3=5, v4="cli_v4"))
    assert settings.sub_model.v1 == "cli_v1"
    assert settings.sub_model.v2 == b"cli_v2"
    assert settings.sub_model.v3 == 5
    assert settings.sub_model.v4 == "cli_v4"


def test_env_prefix():
    os.environ["CDEF_NEW_SETTING"] = "test_value"
    settings = Settings()
    assert hasattr(settings, "NEW_SETTING")
    assert settings.NEW_SETTING == "test_value"
    del os.environ["CDEF_NEW_SETTING"]


def test_secrets_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        secrets_dir = Path(temp_dir) / "secrets"
        secrets_dir.mkdir()
        (secrets_dir / "SECRET_KEY").write_text("super_secret")

        settings = Settings(_secrets_dir=str(secrets_dir))
        assert hasattr(settings, "SECRET_KEY")
        assert settings.SECRET_KEY == "super_secret"
