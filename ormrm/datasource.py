from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class DatasourceMetadata:
    paginated: bool = False
    default_page_size: int | None = None
    max_page_size: int | None = None


@dataclass(frozen=True)
class DataPage(Generic[T]):
    items: list[T]
    page: int
    page_size: int
    total_items: int
    total_pages: int
