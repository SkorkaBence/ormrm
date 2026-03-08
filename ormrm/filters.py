from __future__ import annotations

import abc
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class DateTimeRange:
    start: datetime | None = None
    end: datetime | None = None


class BaseFilter(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def matches(cls, candidate: Any, value: Any) -> bool: ...


class EqualsFilter(BaseFilter):
    @classmethod
    def matches(cls, candidate: Any, value: Any) -> bool:
        return candidate == value


class ContainsFilter(BaseFilter):
    @classmethod
    def matches(cls, candidate: Any, value: Any) -> bool:
        if candidate is None:
            return False
        return str(value).lower() in str(candidate).lower()


class InListFilter(BaseFilter):
    @classmethod
    def matches(cls, candidate: Any, value: Any) -> bool:
        return candidate in value


class DateTimeRangeFilter(BaseFilter):
    @classmethod
    def matches(cls, candidate: Any, value: DateTimeRange) -> bool:
        if candidate is None:
            return False
        if value.start is not None and candidate < value.start:
            return False
        if value.end is not None and candidate > value.end:
            return False
        return True
