from typing import Any

from cdef_cohort.registers.base import BaseProcessor


class ProcessorRegistry:
    def __init__(self):
        self._processors: dict[str, BaseProcessor] = {}

    def register(self, name: str, processor: BaseProcessor) -> None:
        self._processors[name] = processor

    def get(self, name: str) -> BaseProcessor:
        return self._processors[name]

    def process_all(self, **kwargs: Any) -> None:
        """Process all registered processors"""
        for processor in self._processors.values():
            processor.process(**kwargs)


registry = ProcessorRegistry()
