from typing import Any

from cdef_cohort.services.container import ServiceContainer

from .factory import RegisterFactory


class RegisterManager:
    def __init__(self, container: ServiceContainer):
        self.factory = RegisterFactory(container)
        self._register_processors()

    def _register_processors(self) -> None:
        """Register all available processors"""
        from . import akm, bef, idan, ind, uddf
        self.factory.register("akm", akm.AKMProcessor)
        self.factory.register("bef", bef.BEFProcessor)
        self.factory.register("idan", idan.IDANProcessor)
        self.factory.register("ind", ind.INDProcessor)
        self.factory.register("uddf", uddf.UDDFProcessor)

    def process_register(self, register_type: str, **kwargs: Any) -> None:
        """Process a specific register"""
        processor = self.factory.create(register_type)
        processor.process(**kwargs)

    def process_all(self, **kwargs: Any) -> None:
        """Process all registers"""
        for register_type in ["akm", "bef", "idan", "ind", "uddf"]:
            self.process_register(register_type, **kwargs)
