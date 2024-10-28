from pathlib import Path

import pytest

from cdef_cohort.services.container import ServiceContainer


@pytest.fixture
def test_data_dir(tmp_path: Path) -> Path:
    """Create and return a temporary directory for test data"""
    data_dir = tmp_path / "test_data"
    data_dir.mkdir()
    return data_dir

@pytest.fixture
def container(test_data_dir: Path) -> ServiceContainer:
    """Create a configured service container for testing"""
    container = ServiceContainer()

    # Configure services with test settings
    container.configure({
        "services": {
            "config": {
                "mappings_path": test_data_dir / "mappings",
            },
            "register": {
                "registers": {
                    "test_register": {
                        "name": "test_register",
                        "input_files": str(test_data_dir / "input.parquet"),
                        "output_file": str(test_data_dir / "output.parquet"),
                        "schema_def": {"col1": "string", "col2": "int32"},
                        "defaults": {
                            "columns_to_keep": ["col1", "col2"],
                            "longitudinal": True
                        }
                    }
                }
            }
        }
    })

    return container
