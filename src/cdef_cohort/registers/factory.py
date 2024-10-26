
from cdef_cohort.registers.base import BaseProcessor
from cdef_cohort.services.container import ServiceContainer


class RegisterFactory:
    def __init__(self, container: ServiceContainer):
        self.container = container
        self._processors: dict[str, type[BaseProcessor]] = {}
        self._register_processors()

    def register(self, name: str, processor_class: type[BaseProcessor]) -> None:
        """Register a processor class"""
        self._processors[name] = processor_class

    def create(self, register_type: str) -> BaseProcessor:
        """Create a processor instance with services"""
        if register_type not in self._processors:
            raise ValueError(f"Unknown register type: {register_type}")

        processor_class = self._processors[register_type]

        # Special case for LongitudinalProcessor
        if register_type == "longitudinal":
            return processor_class(
                data_service=self.container.data_service,
                event_service=self.container.event_service,
                mapping_service=self.container.mapping_service,
                register_factory=self
            )

        return processor_class(
            data_service=self.container.data_service,
            event_service=self.container.event_service,
            mapping_service=self.container.mapping_service,
        )

    def _register_processors(self) -> None:
        """Register all available processors"""
        from . import (
            akm,
            bef,
            idan,
            ind,
            longitudinal,
            lpr3_diagnoser,
            lpr3_kontakter,
            lpr_adm,
            lpr_bes,
            lpr_diag,
            uddf,
        )

        self.register("longitudinal", longitudinal.LongitudinalProcessor)
        self.register("akm", akm.AKMProcessor)
        self.register("bef", bef.BEFProcessor)
        self.register("idan", idan.IDANProcessor)
        self.register("ind", ind.INDProcessor)
        self.register("uddf", uddf.UDDFProcessor)
        self.register("lpr_adm", lpr_adm.LPRAdmProcessor)
        self.register("lpr_bes", lpr_bes.LPRBesProcessor)
        self.register("lpr_diag", lpr_diag.LPRDiagProcessor)
        self.register("lpr3_diagnoser", lpr3_diagnoser.LPR3DiagnoserProcessor)
        self.register("lpr3_kontakter", lpr3_kontakter.LPR3KontakterProcessor)
