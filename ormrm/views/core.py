from __future__ import annotations

import builtins
from typing import Any

from ..models import BaseModel
from ..plans import ViewExecutionPlan, ViewPlanner

from .execution import ViewExecutionMixin
from .planning import ViewPlanningMixin
from .relations import ViewRelationMixin
from .types import (
    Page,
    PaginationConfig,
    SortByInput,
    ViewField,
    ViewFilter,
    ViewSortable,
)


class View(ViewExecutionMixin, ViewPlanningMixin, ViewRelationMixin):
    fields: builtins.list[ViewField]
    filters: builtins.list[ViewFilter]
    sortables: builtins.list[ViewSortable]
    pagination: PaginationConfig
    root_model: type[BaseModel]
    _filters_by_name: dict[str, ViewFilter]
    _sortables_by_name: dict[str, ViewSortable]

    def __init__(
        self,
        *,
        fields: builtins.list[ViewField],
        filters: builtins.list[ViewFilter] | None = None,
        sortables: builtins.list[ViewSortable] | None = None,
        pagination: PaginationConfig | None = None,
    ) -> None:
        if not fields:
            raise ValueError("A view requires at least one field")

        self.fields = fields
        self.filters = filters or []
        self.sortables = sortables or []
        self.pagination = pagination or PaginationConfig()
        self.root_model = self._model_for_field(fields[0].source)
        self._filters_by_name = {item.name: item for item in self.filters}
        self._sortables_by_name = {item.name: item for item in self.sortables}
        self._validate()

    def list(
        self,
        *,
        page: int = 1,
        page_size: int | None = None,
        sort_by: SortByInput = None,
        filters: dict[str, Any] | None = None,
    ) -> Page:
        plan = self.inspect(page=page, page_size=page_size, sort_by=sort_by, filters=filters)
        return self._execute_plan(plan)

    async def list_async(
        self,
        *,
        page: int = 1,
        page_size: int | None = None,
        sort_by: SortByInput = None,
        filters: dict[str, Any] | None = None,
    ) -> Page:
        plan = self.inspect(page=page, page_size=page_size, sort_by=sort_by, filters=filters)
        return await self._execute_plan_async(plan)

    def inspect(
        self,
        *,
        page: int = 1,
        page_size: int | None = None,
        sort_by: SortByInput = None,
        filters: dict[str, Any] | None = None,
    ) -> ViewExecutionPlan:
        return ViewPlanner(self).build(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            filters=filters,
        )

    build_plan = inspect


def create_view(
    *,
    fields: builtins.list[ViewField],
    filters: builtins.list[ViewFilter] | None = None,
    sortables: builtins.list[ViewSortable] | None = None,
    pagination: PaginationConfig | None = None,
) -> View:
    return View(fields=fields, filters=filters, sortables=sortables, pagination=pagination)
