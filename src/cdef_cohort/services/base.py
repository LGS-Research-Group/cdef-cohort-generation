from abc import ABC, abstractmethod
from typing import Any


class BaseService(ABC):
    """Base interface for all services"""

    @abstractmethod
    def initialize(self) -> None:
        """Initialize service resources"""
        pass

    @abstractmethod
    def shutdown(self) -> None:
        """Clean up service resources"""
        pass

    @abstractmethod
    def check_valid(self) -> bool:
        """Validate service configuration"""
        pass

class ConfigurableService(BaseService):
    """Base interface for services that require configuration"""

    @abstractmethod
    def configure(self, config: dict[str, Any]) -> None:
        """Configure the service with provided settings"""
        pass
