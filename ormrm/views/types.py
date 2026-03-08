from __future__ import annotations

import builtins
from dataclasses import dataclass
from typing import Any

from ..filters import BaseFilter
from ..models import BaseModel
from ..query import ModelQuery
from ..schema import FieldDefinition


@dataclass(frozen=True)
class ViewField:
    name: str
    source: FieldDefinition


@dataclass(frozen=True)
class ViewFilter:
    name: str
    source: FieldDefinition
    filter_type: type[BaseFilter]


@dataclass(frozen=True)
class ViewSortable:
    name: str
    source: FieldDefinition
    default_sort: str | None = None


@dataclass(frozen=True)
class PaginationConfig:
    default_page_size: int = 20
    max_page_size: int = 100


@dataclass(frozen=True)
class Page:
    items: builtins.list[dict[str, Any]]
    page: int
    page_size: int
    total_items: int
    total_pages: int


@dataclass(frozen=True)
class RelationStep:
    current_model: type[BaseModel]
    next_model: type[BaseModel]
    current_field_name: str
    next_field_name: str
    relation_name: str


@dataclass
class PreparedRootQuery:
    query: ModelQuery
    model_cache: dict[type[BaseModel], dict[Any, BaseModel]]
    impossible: bool = False


SortByInput = str | builtins.list[str] | tuple[str, ...] | None
PathIndex = dict[Any, builtins.list[BaseModel]]
PathCache = dict[tuple[RelationStep, ...], builtins.list[PathIndex]]
