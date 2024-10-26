import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import polars as pl
from pydantic import BaseModel


class MappingConfig(BaseModel):
    mapping_dir: Path
    cache_enabled: bool = False
    cache_ttl: int = 3600

class MappingCache(ABC):
    @abstractmethod
    def get(self, key: str) -> dict[str, Any] | None:
        pass

    @abstractmethod
    def set(self, key: str, value: dict[str, Any]) -> None:
        pass

class InMemoryCache(MappingCache):
    def __init__(self):
        self._cache = {}

    def get(self, key: str) -> dict[str, Any] | None:
        return self._cache.get(key)

    def set(self, key: str, value: dict[str, Any]) -> None:
        self._cache[key] = value

class MappingService:
    def __init__(self, config: MappingConfig, cache: MappingCache | None = None):
        self.config = config
        self.cache = cache or InMemoryCache()

    def get_mapping(self, name: str) -> dict[str, Any]:
        if self.config.cache_enabled:
            cached = self.cache.get(name)
            if cached:
                return cached

        mapping_file = self.config.mapping_dir / f"{name}.json"
        with open(mapping_file) as f:
            mapping = json.load(f)

        if self.config.cache_enabled:
            self.cache.set(name, mapping)

        return mapping

    def apply_mapping(
        self,
        col: pl.Expr,
        mapping_name: str,
        return_dtype: type[pl.DataType] = pl.Categorical
    ) -> pl.Expr:
        mapping = self.get_mapping(mapping_name)
        return col.map_elements(
            lambda x: mapping.get(str(x), x),
            return_dtype=return_dtype
        )
