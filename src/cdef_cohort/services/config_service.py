from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from .base import ConfigurableService


class RegisterConfig(BaseModel):
    name: str
    input_files: str
    output_file: str
    schema_def: dict[str, str]
    defaults: dict[str, Any]

class ConfigServiceModel(BaseModel):
    register_configs: dict[str, RegisterConfig] = Field(default_factory=dict)
    mappings_path: Path

class ConfigService(ConfigurableService):
    """Central configuration service"""

    def __init__(self, *, mappings_path: Path):
        self.model = ConfigServiceModel(
            mappings_path=mappings_path,
            register_configs={}
        )

    def initialize(self) -> None:
        """Initialize configuration service"""
        if not self.check_valid():
            raise ValueError("Invalid configuration")

    def shutdown(self) -> None:
        """Clean up configuration service"""
        pass

    @property
    def mappings_path(self) -> Path:
        return self.model.mappings_path

    @property
    def register_configs(self) -> dict[str, RegisterConfig]:
        return self.model.register_configs

    def configure(self, config: dict[str, Any]) -> None:
        """Configure service with new settings"""
        if "mappings_path" in config:
            self.model.mappings_path = Path(config["mappings_path"])
        if "register_configs" in config:
            self.model.register_configs.update(config["register_configs"])

        if not self.check_valid():
            raise ValueError("Invalid configuration")

    def check_valid(self) -> bool:
        """Check if configuration is valid"""
        try:
            # Validate paths exist
            if not self.mappings_path.exists():
                return False

            # Check register configs are valid
            for config in self.register_configs.values():
                # Validate register schema definitions
                if not config.schema_def or not isinstance(config.schema_def, dict):
                    return False

                # Validate input/output paths
                input_path = Path(config.input_files)
                output_path = Path(config.output_file)

                if not input_path.parent.exists():
                    return False
                if not list(input_path.parent.glob(input_path.name)):
                    return False

                # Ensure output directory can be created
                try:
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                except (OSError, PermissionError):
                    return False

                # Validate defaults have required keys
                required_defaults = {"columns_to_keep", "join_parents_only", "longitudinal"}
                if not all(key in config.defaults for key in required_defaults):
                    return False

            return True

        except Exception:
            return False

    def get_register_config(self, register_name: str) -> dict[str, Any]:
        """Get config for specific register"""
        if register_name not in self.register_configs:
            raise KeyError(f"No configuration found for register: {register_name}")
        return self.register_configs[register_name].model_dump()
