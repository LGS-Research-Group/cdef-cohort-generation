from abc import ABC, abstractmethod
from typing import Any


class BaseService(ABC):
    """Base interface for all services"""

    @abstractmethod
    def initialize(self) -> None:
        """Initialize service resources

        This method should be called before using the service.
        It sets up any necessary resources or connections.

        Raises:
            ValueError: If the service is not properly configured
        """
        pass

    @abstractmethod
    def shutdown(self) -> None:
        """Clean up service resources

        This method should be called when the service is no longer needed.
        It ensures proper cleanup of resources.
        """
        pass

    @abstractmethod
    def check_valid(self) -> bool:
        """Validate service configuration

        Returns:
            bool: True if the service is properly configured, False otherwise
        """
        pass


class ConfigurableService(BaseService):
    """Base interface for services that require configuration"""

    @abstractmethod
    def configure(self, config: dict[str, Any]) -> None:
        """Configure the service with provided settings

        Args:
            config: Configuration dictionary with service-specific settings

        Raises:
            ValueError: If the configuration is invalid
        """
        pass
