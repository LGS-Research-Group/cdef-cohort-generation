from pathlib import Path
from typing import Any


def create_standard_config(base_dir: Path) -> dict[str, Any]:
    """Create a standard configuration for common use cases

    Args:
        base_dir: Base directory for data files

    Returns:
        dict: Complete configuration for all services
    """
    return {
        "services": {
            "config": {
                "mappings_path": base_dir / "mappings",
            },
            "register_configs": {
                "bef": {
                    "name": "bef",
                    "input_files": str(base_dir / "registers/bef/*.parquet"),
                    "output_file": str(base_dir / "processed/bef.parquet"),
                    "schema_def": {"PNR": "string", "FOED_DAG": "date", "FAR_ID": "string", "MOR_ID": "string"},
                    "defaults": {"columns_to_keep": ["PNR", "FOED_DAG", "FAR_ID", "MOR_ID"], "longitudinal": True},
                }
                # Add other register configurations...
            },
            "pipeline_config": {
                "stage_order": ["population", "health", "longitudinal"],
                "output_configs": {"final_cohort": str(base_dir / "cohort.parquet")},
            },
        }
    }
