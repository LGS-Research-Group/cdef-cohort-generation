from pathlib import Path

from cdef_cohort.services.config_service import ConfigService

config_service = ConfigService(
    mappings_path=Path(__file__).parent / "mappings"
)
