from pathlib import Path
from typing import cast

from .base import BaseService
from .config_service import ConfigService
from .data_service import DataService
from .event_service import EventService
from .mapping_service import MappingService
from .population_service import PopulationService


class ServiceContainer:
    """Container for managing service instances and dependencies."""

    def __init__(self):
        """Initialize the service container with empty services dict."""
        self._services: dict[str, BaseService] = {}
        self._config = ConfigService(mappings_path=Path(__file__).parent.parent / "mappings")

    def initialize(self) -> None:
        """Initialize all services"""
        for service in self._services.values():
            service.initialize()

    def shutdown(self) -> None:
        """Shutdown all services"""
        for service in self._services.values():
            service.shutdown()

    @property
    def config(self) -> ConfigService:
        """Get the config service instance."""
        return self._config

    @property
    def population_service(self) -> PopulationService:
        """Get or create the population service instance."""
        if "population" not in self._services:
            self._services["population"] = PopulationService(self.data_service)
        return cast(PopulationService, self._services["population"])

    @property
    def mapping_service(self) -> MappingService:
        """Get or create the mapping service instance."""
        if "mapping" not in self._services:
            self._services["mapping"] = MappingService(self._config.mappings_path)
        return cast(MappingService, self._services["mapping"])

    @property
    def data_service(self) -> DataService:
        """Get or create the data service instance."""
        if "data" not in self._services:
            self._services["data"] = DataService()
        return cast(DataService, self._services["data"])

    @property
    def event_service(self) -> EventService:
        """Get or create the event service instance."""
        if "event" not in self._services:
            self._services["event"] = EventService()
        return cast(EventService, self._services["event"])

    def reset(self) -> None:
        """Reset all services (useful for testing)."""
        self.shutdown()
        self._services.clear()
        self._config = ConfigService(mappings_path=Path(__file__).parent.parent / "mappings")

    def get_service(self, service_name: str) -> BaseService:
        """Get a service by name."""
        if service_name not in self._services:
            raise KeyError(f"Service not found: {service_name}")
        return self._services[service_name]

    def register_service(self, service_name: str, service: BaseService) -> None:
        """Register a new service."""
        self._services[service_name] = service


# Create global container instance
container = ServiceContainer()


def get_container() -> ServiceContainer:
    """Get the global service container instance."""
    return container
