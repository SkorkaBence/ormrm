from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class DatasourceMetadata:
    paginated: bool = False
    default_page_size: int | None = None
    max_page_size: int | None = None

    def __post_init__(self) -> None:
        if self.paginated:
            if self.default_page_size is None:
                raise ValueError("default_page_size must be set if paginated is True")
            if self.max_page_size is None:
                raise ValueError("max_page_size must be set if paginated is True")

            if self.default_page_size < 1:
                raise ValueError("default_page_size must be at least 1")
            if self.max_page_size < 1:
                raise ValueError("max_page_size must be at least 1")
        else:
            if self.default_page_size is not None:
                raise ValueError("default_page_size must be None if paginated is False")
            if self.max_page_size is not None:
                raise ValueError("max_page_size must be None if paginated is False")


@dataclass(frozen=True)
class DataPage(Generic[T]):
    items: list[T]
    page: int
    page_size: int
    total_items: int
    total_pages: int
