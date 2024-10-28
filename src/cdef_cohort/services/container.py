from pathlib import Path
from typing import Any

from .analytical_data_service import AnalyticalDataService
from .base import BaseService
from .cohort_service import CohortService
from .config_service import ConfigService
from .data_service import DataService
from .event_service import EventService
from .mapping_service import MappingService
from .pipeline_service import PipelineService
from .population_service import PopulationService
from .register_service import RegisterService
from .table_service import TableService


class ServiceContainer:
    """Container for managing service instances and dependencies."""

    def __init__(self) -> None:
        """Initialize the service container."""
        mappings_path = Path(__file__).parent.parent / "mappings"

        # Initialize all services explicitly
        self._config_service: ConfigService = ConfigService(mappings_path=mappings_path)
        self._data_service: DataService | None = None
        self._event_service: EventService | None = None
        self._mapping_service: MappingService | None = None
        self._population_service: PopulationService | None = None
        self._register_service: RegisterService | None = None
        self._pipeline_service: PipelineService | None = None
        self._cohort_service: CohortService | None = None
        self._analytical_data_service: AnalyticalDataService | None = None
        self._table_service: TableService | None = None

        # Dictionary for additional services
        self._services: dict[str, BaseService] = {}

    def initialize(self) -> None:
        """Initialize all services"""
        self._config_service.initialize()

        # Initialize other services if they exist
        services = [
            self._data_service,
            self._event_service,
            self._mapping_service,
            self._population_service,
            self._register_service,
            self._pipeline_service,
            self._cohort_service,
            self._table_service,
        ]

        for service in services:
            if service is not None:
                service.initialize()

        for service in self._services.values():
            service.initialize()

    def shutdown(self) -> None:
        """Shutdown all services"""
        self._config_service.shutdown()

        # Shutdown other services if they exist
        services = [
            self._data_service,
            self._event_service,
            self._mapping_service,
            self._population_service,
            self._register_service,
            self._pipeline_service,
            self._cohort_service,
            self._table_service,
        ]

        for service in services:
            if service is not None:
                service.shutdown()

        for service in self._services.values():
            service.shutdown()

    def get_table_service(self) -> TableService:
        """Get or create the table service instance."""
        if self._table_service is None:
            self._table_service = TableService(self.get_data_service())
        return self._table_service

    def get_config_service(self) -> ConfigService:
        """Get the config service instance."""
        return self._config_service

    def get_data_service(self) -> DataService:
        """Get or create the data service instance."""
        if self._data_service is None:
            self._data_service = DataService()
        return self._data_service

    def get_event_service(self) -> EventService:
        """Get or create the event service instance."""
        if self._event_service is None:
            self._event_service = EventService()
        return self._event_service

    def get_mapping_service(self) -> MappingService:
        """Get or create the mapping service instance."""
        if self._mapping_service is None:
            self._mapping_service = MappingService(self._config_service.mappings_path)
        return self._mapping_service

    def get_population_service(self) -> PopulationService:
        """Get or create the population service instance."""
        if self._population_service is None:
            self._population_service = PopulationService(self.get_data_service())
        return self._population_service

    def get_register_service(self) -> RegisterService:
        """Get or create the register service instance."""
        if self._register_service is None:
            self._register_service = RegisterService(
                data_service=self.get_data_service(), config_service=self._config_service
            )
        return self._register_service

    def get_pipeline_service(self) -> PipelineService:
        """Get or create the pipeline service instance."""
        if self._pipeline_service is None:
            self._pipeline_service = PipelineService(
                register_service=self.get_register_service(),
                cohort_service=self.get_cohort_service(),
                population_service=self.get_population_service(),
                data_service=self.get_data_service(),
                analytical_data_service=self.get_analytical_data_service(),
            )
        return self._pipeline_service

    def get_cohort_service(self) -> CohortService:
        """Get or create the cohort service instance."""
        if self._cohort_service is None:
            self._cohort_service = CohortService(
                data_service=self.get_data_service(),
                event_service=self.get_event_service(),
                mapping_service=self.get_mapping_service(),
            )
        return self._cohort_service

    def get_analytical_data_service(self) -> AnalyticalDataService:
        """Get or create the analytical data service instance."""
        if self._analytical_data_service is None:
            self._analytical_data_service = AnalyticalDataService(self.get_data_service(), self.get_cohort_service())
        return self._analytical_data_service

    def configure(self, config: dict[str, Any]) -> None:
        """Configure all services with provided configuration"""
        if "services" not in config:
            raise ValueError("Configuration must contain 'services' section")

        service_configs = config["services"]

        # Configure each service if configuration is provided
        if "config" in service_configs:
            self.get_config_service().configure(service_configs["config"])
        if "register" in service_configs:
            self.get_register_service().configure(service_configs["register"])
        if "pipeline" in service_configs:
            self.get_pipeline_service().configure(service_configs["pipeline"])

    def reset(self) -> None:
        """Reset all services (useful for testing)."""
        self.shutdown()
        self._services.clear()

        # Reset all service instances
        mappings_path = Path(__file__).parent.parent / "mappings"
        self._config_service = ConfigService(mappings_path=mappings_path)
        self._data_service = None
        self._event_service = None
        self._mapping_service = None
        self._population_service = None
        self._register_service = None
        self._pipeline_service = None
        self._cohort_service = None

    def get_service(self, service_name: str) -> BaseService:
        """Get a service by name."""
        if service_name not in self._services:
            raise KeyError(f"Service not found: {service_name}")
        return self._services[service_name]


# Create global container instance
_container = ServiceContainer()


def get_container() -> ServiceContainer:
    """Get the global service container instance."""
    return _container
