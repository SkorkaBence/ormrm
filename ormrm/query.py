from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from .filters import BaseFilter
from .schema import FieldDefinition


def _format_value(value: Any) -> str:
    if isinstance(value, str):
        return repr(value)
    if isinstance(value, set):
        ordered = sorted(value)
        return "{" + ", ".join(repr(item) for item in ordered) + "}"
    return repr(value)


@dataclass(frozen=True)
class BoundFilter:
    source: FieldDefinition
    filter_type: type[BaseFilter]
    value: Any

    @property
    def field_name(self) -> str:
        if self.source.name is None:
            raise ValueError("Bound filter uses an unbound field")
        return self.source.name

    def __repr__(self) -> str:
        return f"{self.field_name} {self.filter_type.__name__} {_format_value(self.value)}"


class ModelQuery:
    def __init__(self, filters: dict[str, tuple[BoundFilter, ...]] | None = None) -> None:
        self._filters = filters or {}

    @classmethod
    def from_bound_filters(cls, bound_filters: Iterable[BoundFilter]) -> "ModelQuery":
        grouped: dict[str, list[BoundFilter]] = {}
        for bound_filter in bound_filters:
            grouped.setdefault(bound_filter.field_name, []).append(bound_filter)
        return cls({name: tuple(filters) for name, filters in grouped.items()})

    def for_field(self, field_name: str) -> tuple[BoundFilter, ...]:
        return self._filters.get(field_name, ())

    def items(self) -> tuple[tuple[str, tuple[BoundFilter, ...]], ...]:
        return tuple(self._filters.items())

    def is_empty(self) -> bool:
        return not self._filters

    def bound_filters(self) -> tuple[BoundFilter, ...]:
        flattened: list[BoundFilter] = []
        for _, filters in self.items():
            flattened.extend(filters)
        return tuple(flattened)

    def __repr__(self) -> str:
        if self.is_empty():
            return "[]"
        return "[" + ", ".join(repr(bound_filter) for bound_filter in self.bound_filters()) + "]"
