from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from cdef_cohort.logging_config import logger

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
        self.model = ConfigServiceModel(mappings_path=mappings_path, register_configs={})
        # Add this line to create the mappings directory if it doesn't exist
        mappings_path.mkdir(parents=True, exist_ok=True)

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
            new_configs = {}
            for name, cfg in config["register_configs"].items():
                # Create RegisterConfig instance for each configuration
                register_config = RegisterConfig(
                    name=name,
                    input_files=cfg["input_files"],
                    output_file=cfg["output_file"],
                    schema_def=cfg.get("schema_def", {}),
                    defaults=cfg["defaults"],
                )
                new_configs[name] = register_config

            self.model.register_configs.update(new_configs)

        if not self.check_valid():
            raise ValueError("Invalid configuration")

    def check_valid(self) -> bool:
        """Check if configuration is valid"""
        try:
            # Validate mappings path exists
            if not self.mappings_path.exists():
                logger.error(f"Mappings path does not exist: {self.mappings_path}")
                return False

            # Check if mappings path is a directory
            if not self.mappings_path.is_dir():
                logger.error(f"Mappings path is not a directory: {self.mappings_path}")
                return False

            # For register configs, only validate if they exist
            if self.register_configs:
                for name, config in self.register_configs.items():
                    # Validate register schema definitions
                    if not config.schema_def or not isinstance(config.schema_def, dict):
                        logger.error(f"Invalid schema definition for register {name}")
                        return False

                    # Validate defaults have required keys
                    required_defaults = {"columns_to_keep", "join_parents_only", "longitudinal"}
                    missing_defaults = required_defaults - set(config.defaults.keys())
                    if missing_defaults:
                        logger.error(f"Missing required defaults for register {name}: {missing_defaults}")
                        return False

            return True

        except Exception as e:
            logger.error(f"Error during configuration validation: {str(e)}")
            return False

    def get_register_config(self, register_name: str) -> dict[str, Any]:
        """Get config for specific register"""
        if register_name not in self.register_configs:
            raise KeyError(f"No configuration found for register: {register_name}")
        return self.register_configs[register_name].model_dump()
