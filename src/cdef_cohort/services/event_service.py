import polars as pl

from .base import BaseService


class EventService(BaseService):
    def __init__(self):
        self._event_definitions = {}

    def initialize(self) -> None:
        """Initialize service resources"""
        pass

    def shutdown(self) -> None:
        """Clean up service resources"""
        self._event_definitions.clear()

    def check_valid(self) -> bool:
        """Validate service configuration"""
        return True

    def register_event(self, name: str, definition: pl.Expr) -> None:
        """Register a new event definition"""
        self._event_definitions[name] = definition

    def identify_events(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Identify events in the dataframe"""
        events = []
        for name, expr in self._event_definitions.items():
            event = df.filter(expr).with_columns(pl.lit(name).alias("event_type"))
            events.append(event)
        return pl.concat(events) if events else df
