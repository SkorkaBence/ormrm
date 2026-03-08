from __future__ import annotations

import builtins
import inspect
import math
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Awaitable, cast

from .datasource import DataPage
from .filters import EqualsFilter, InListFilter
from .models import BaseModel
from .plans import ViewExecutionPlan, ViewPlanner
from .query import BoundFilter, ModelQuery
from .schema import FieldDefinition, RelationDefinition


@dataclass(frozen=True)
class ViewField:
    name: str
    source: FieldDefinition


@dataclass(frozen=True)
class ViewFilter:
    name: str
    source: FieldDefinition
    filter_type: type


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


class View:

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

    def _execute_plan(self, plan: ViewExecutionPlan) -> Page:
        prepared = self._prepare_root_query(plan.grouped_filter_map())
        if prepared.impossible:
            return self._empty_page(plan.request.page, plan.request.page_size)

        model_cache = prepared.model_cache
        path_cache: PathCache = {}

        if plan.execution_mode == "root_page":
            root_page = self._fetch_data_page(
                self.root_model,
                prepared.query,
                page=plan.request.page,
                page_size=plan.request.page_size,
                sort_by=self._sort_by_argument(plan.root_sort_by),
            )
            self._cache_records(model_cache, self.root_model, root_page.items)
            items = [
                {
                    field.name: self._resolve_source_value(
                        record,
                        field.source,
                        root_page.items,
                        path_cache,
                        model_cache,
                    )
                    for field in self.fields
                }
                for record in root_page.items
            ]
            total_pages = (
                math.ceil(root_page.total_items / plan.request.page_size)
                if root_page.total_items
                else 0
            )
            return Page(
                items=items,
                page=plan.request.page,
                page_size=plan.request.page_size,
                total_items=root_page.total_items,
                total_pages=total_pages,
            )

        if plan.execution_mode == "grouped_root_page":
            return self._execute_grouped_plan(plan, prepared)

        collected_root_page = self._collect_all_records(
            self.root_model,
            prepared.query,
            sort_by=None,
            model_cache=model_cache,
        )
        root_records = self._sort_root_records_by_source(
            collected_root_page.items,
            plan.sort_sources,
            plan.descending_flags,
            path_cache,
            model_cache,
        )
        total_items = len(root_records)
        total_pages = math.ceil(total_items / plan.request.page_size) if total_items else 0
        start = (plan.request.page - 1) * plan.request.page_size
        end = start + plan.request.page_size
        paged_records = root_records[start:end]
        items = [
            {
                field.name: self._resolve_source_value(
                    record,
                    field.source,
                    root_records,
                    path_cache,
                    model_cache,
                )
                for field in self.fields
            }
            for record in paged_records
        ]
        return Page(
            items=items,
            page=plan.request.page,
            page_size=plan.request.page_size,
            total_items=total_items,
            total_pages=total_pages,
        )

    async def _execute_plan_async(self, plan: ViewExecutionPlan) -> Page:
        prepared = await self._prepare_root_query_async(plan.grouped_filter_map())
        if prepared.impossible:
            return self._empty_page(plan.request.page, plan.request.page_size)

        model_cache = prepared.model_cache
        path_cache: PathCache = {}

        if plan.execution_mode == "root_page":
            root_page = await self._fetch_data_page_async(
                self.root_model,
                prepared.query,
                page=plan.request.page,
                page_size=plan.request.page_size,
                sort_by=self._sort_by_argument(plan.root_sort_by),
            )
            self._cache_records(model_cache, self.root_model, root_page.items)
            items = []
            for record in root_page.items:
                item = {
                    field.name: await self._resolve_source_value_async(
                        record,
                        field.source,
                        root_page.items,
                        path_cache,
                        model_cache,
                    )
                    for field in self.fields
                }
                items.append(item)
            total_pages = (
                math.ceil(root_page.total_items / plan.request.page_size)
                if root_page.total_items
                else 0
            )
            return Page(
                items=items,
                page=plan.request.page,
                page_size=plan.request.page_size,
                total_items=root_page.total_items,
                total_pages=total_pages,
            )

        if plan.execution_mode == "grouped_root_page":
            return await self._execute_grouped_plan_async(plan, prepared)

        collected_root_page = await self._collect_all_records_async(
            self.root_model,
            prepared.query,
            sort_by=None,
            model_cache=model_cache,
        )
        root_records = await self._sort_root_records_by_source_async(
            collected_root_page.items,
            plan.sort_sources,
            plan.descending_flags,
            path_cache,
            model_cache,
        )
        total_items = len(root_records)
        total_pages = math.ceil(total_items / plan.request.page_size) if total_items else 0
        start = (plan.request.page - 1) * plan.request.page_size
        end = start + plan.request.page_size
        paged_records = root_records[start:end]
        items = []
        for record in paged_records:
            item = {
                field.name: await self._resolve_source_value_async(
                    record,
                    field.source,
                    root_records,
                    path_cache,
                    model_cache,
                )
                for field in self.fields
            }
            items.append(item)
        return Page(
            items=items,
            page=plan.request.page,
            page_size=plan.request.page_size,
            total_items=total_items,
            total_pages=total_pages,
        )

    def _execute_grouped_plan(self, plan: ViewExecutionPlan, prepared: PreparedRootQuery) -> Page:
        group_source = plan.sort_sources[0]
        group_model = self._model_for_field(group_source)
        group_query = ModelQuery.from_bound_filters(plan.grouped_filter_map().get(group_model, ()))
        group_sort = ("-" if plan.descending_flags[0] else "") + self._field_name(group_source)
        group_page_size = (
            group_model.datasource.max_page_size or group_model.datasource.default_page_size
        )
        root_sort = self._sort_by_argument(plan.root_sort_by)
        model_cache = prepared.model_cache

        selected_records: builtins.list[BaseModel] = []
        total_items = 0
        offset = (plan.request.page - 1) * plan.request.page_size
        needed = plan.request.page_size

        first_path = self._find_path(self.root_model, group_model)
        if len(first_path) != 1:
            raise ValueError("Grouped root execution requires a direct relation path")
        join_step = first_path[0]
        group_pk_name = group_model.primary_key_field().bound_name

        group_page_number = 1
        total_group_pages = 1
        while group_page_number <= total_group_pages:
            group_page = self._fetch_data_page(
                group_model,
                group_query,
                page=group_page_number,
                page_size=group_page_size,
                sort_by=group_sort,
            )
            total_group_pages = group_page.total_pages
            self._cache_records(model_cache, group_model, group_page.items)

            for group_record in group_page.items:
                group_records, group_total = self._collect_group_page_records(
                    group_record=group_record,
                    group_pk_name=group_pk_name,
                    join_step=join_step,
                    root_query=prepared.query,
                    root_sort=root_sort,
                    offset=offset,
                    needed=needed,
                    consumed_before=total_items,
                    model_cache=model_cache,
                )
                total_items += group_total
                if group_records:
                    selected_records.extend(group_records)
                    needed -= len(group_records)

            group_page_number += 1

        path_cache: PathCache = {}
        items = [
            {
                field.name: self._resolve_source_value(
                    record,
                    field.source,
                    selected_records,
                    path_cache,
                    model_cache,
                )
                for field in self.fields
            }
            for record in selected_records
        ]
        total_pages = math.ceil(total_items / plan.request.page_size) if total_items else 0
        return Page(
            items=items,
            page=plan.request.page,
            page_size=plan.request.page_size,
            total_items=total_items,
            total_pages=total_pages,
        )

    async def _execute_grouped_plan_async(
        self, plan: ViewExecutionPlan, prepared: PreparedRootQuery
    ) -> Page:
        group_source = plan.sort_sources[0]
        group_model = self._model_for_field(group_source)
        group_query = ModelQuery.from_bound_filters(plan.grouped_filter_map().get(group_model, ()))
        group_sort = ("-" if plan.descending_flags[0] else "") + self._field_name(group_source)
        group_page_size = (
            group_model.datasource.max_page_size or group_model.datasource.default_page_size
        )
        root_sort = self._sort_by_argument(plan.root_sort_by)
        model_cache = prepared.model_cache

        selected_records: builtins.list[BaseModel] = []
        total_items = 0
        offset = (plan.request.page - 1) * plan.request.page_size
        needed = plan.request.page_size

        first_path = self._find_path(self.root_model, group_model)
        if len(first_path) != 1:
            raise ValueError("Grouped root execution requires a direct relation path")
        join_step = first_path[0]
        group_pk_name = group_model.primary_key_field().bound_name

        group_page_number = 1
        total_group_pages = 1
        while group_page_number <= total_group_pages:
            group_page = await self._fetch_data_page_async(
                group_model,
                group_query,
                page=group_page_number,
                page_size=group_page_size,
                sort_by=group_sort,
            )
            total_group_pages = group_page.total_pages
            self._cache_records(model_cache, group_model, group_page.items)

            for group_record in group_page.items:
                group_records, group_total = await self._collect_group_page_records_async(
                    group_record=group_record,
                    group_pk_name=group_pk_name,
                    join_step=join_step,
                    root_query=prepared.query,
                    root_sort=root_sort,
                    offset=offset,
                    needed=needed,
                    consumed_before=total_items,
                    model_cache=model_cache,
                )
                total_items += group_total
                if group_records:
                    selected_records.extend(group_records)
                    needed -= len(group_records)

            group_page_number += 1

        path_cache: PathCache = {}
        items = []
        for record in selected_records:
            item = {
                field.name: await self._resolve_source_value_async(
                    record,
                    field.source,
                    selected_records,
                    path_cache,
                    model_cache,
                )
                for field in self.fields
            }
            items.append(item)
        total_pages = math.ceil(total_items / plan.request.page_size) if total_items else 0
        return Page(
            items=items,
            page=plan.request.page,
            page_size=plan.request.page_size,
            total_items=total_items,
            total_pages=total_pages,
        )

    def _collect_group_page_records(
        self,
        *,
        group_record: BaseModel,
        group_pk_name: str,
        join_step: RelationStep,
        root_query: ModelQuery,
        root_sort: str | tuple[str, ...] | None,
        offset: int,
        needed: int,
        consumed_before: int,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> tuple[builtins.list[BaseModel], int]:
        author_query = self._append_bound_filters(
            root_query,
            (
                BoundFilter(
                    source=self.root_model.field(join_step.current_field_name),
                    filter_type=EqualsFilter,
                    value=getattr(group_record, group_pk_name),
                ),
            ),
        )
        initial_page_size = 1 if consumed_before >= offset + needed else max(needed, 1)
        first_page = self._fetch_data_page(
            self.root_model,
            author_query,
            page=1,
            page_size=initial_page_size,
            sort_by=root_sort,
        )
        self._cache_records(model_cache, self.root_model, first_page.items)
        total_for_group = first_page.total_items
        if total_for_group == 0:
            return [], 0
        if consumed_before + total_for_group <= offset or needed <= 0:
            return [], total_for_group

        start_within_group = max(0, offset - consumed_before)
        take_count = min(needed, total_for_group - start_within_group)
        return (
            self._extract_group_window_sync(
                query=author_query,
                root_sort=root_sort,
                start_within_group=start_within_group,
                take_count=take_count,
                seed_page=first_page,
                model_cache=model_cache,
            ),
            total_for_group,
        )

    async def _collect_group_page_records_async(
        self,
        *,
        group_record: BaseModel,
        group_pk_name: str,
        join_step: RelationStep,
        root_query: ModelQuery,
        root_sort: str | tuple[str, ...] | None,
        offset: int,
        needed: int,
        consumed_before: int,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> tuple[builtins.list[BaseModel], int]:
        author_query = self._append_bound_filters(
            root_query,
            (
                BoundFilter(
                    source=self.root_model.field(join_step.current_field_name),
                    filter_type=EqualsFilter,
                    value=getattr(group_record, group_pk_name),
                ),
            ),
        )
        initial_page_size = 1 if consumed_before >= offset + needed else max(needed, 1)
        first_page = await self._fetch_data_page_async(
            self.root_model,
            author_query,
            page=1,
            page_size=initial_page_size,
            sort_by=root_sort,
        )
        self._cache_records(model_cache, self.root_model, first_page.items)
        total_for_group = first_page.total_items
        if total_for_group == 0:
            return [], 0
        if consumed_before + total_for_group <= offset or needed <= 0:
            return [], total_for_group

        start_within_group = max(0, offset - consumed_before)
        take_count = min(needed, total_for_group - start_within_group)
        return (
            await self._extract_group_window_async(
                query=author_query,
                root_sort=root_sort,
                start_within_group=start_within_group,
                take_count=take_count,
                seed_page=first_page,
                model_cache=model_cache,
            ),
            total_for_group,
        )

    def _append_bound_filters(
        self,
        query: ModelQuery,
        extra_filters: tuple[BoundFilter, ...],
    ) -> ModelQuery:
        return ModelQuery.from_bound_filters(query.bound_filters() + extra_filters)

    def _extract_group_window_sync(
        self,
        *,
        query: ModelQuery,
        root_sort: str | tuple[str, ...] | None,
        start_within_group: int,
        take_count: int,
        seed_page: DataPage[BaseModel],
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> builtins.list[BaseModel]:
        if take_count <= 0:
            return []
        page_size = max(seed_page.page_size, 1)
        target_page = start_within_group // page_size + 1
        offset_in_page = start_within_group % page_size
        current_page = seed_page
        if target_page != seed_page.page or seed_page.page_size != page_size:
            current_page = self._fetch_data_page(
                self.root_model,
                query,
                page=target_page,
                page_size=page_size,
                sort_by=root_sort,
            )
            self._cache_records(model_cache, self.root_model, current_page.items)

        items: builtins.list[BaseModel] = []
        while len(items) < take_count and current_page.items:
            items.extend(current_page.items[offset_in_page:])
            if len(items) >= take_count:
                break
            target_page += 1
            offset_in_page = 0
            current_page = self._fetch_data_page(
                self.root_model,
                query,
                page=target_page,
                page_size=page_size,
                sort_by=root_sort,
            )
            self._cache_records(model_cache, self.root_model, current_page.items)
        return items[:take_count]

    async def _extract_group_window_async(
        self,
        *,
        query: ModelQuery,
        root_sort: str | tuple[str, ...] | None,
        start_within_group: int,
        take_count: int,
        seed_page: DataPage[BaseModel],
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> builtins.list[BaseModel]:
        if take_count <= 0:
            return []
        page_size = max(seed_page.page_size, 1)
        target_page = start_within_group // page_size + 1
        offset_in_page = start_within_group % page_size
        current_page = seed_page
        if target_page != seed_page.page or seed_page.page_size != page_size:
            current_page = await self._fetch_data_page_async(
                self.root_model,
                query,
                page=target_page,
                page_size=page_size,
                sort_by=root_sort,
            )
            self._cache_records(model_cache, self.root_model, current_page.items)

        items: builtins.list[BaseModel] = []
        while len(items) < take_count and current_page.items:
            items.extend(current_page.items[offset_in_page:])
            if len(items) >= take_count:
                break
            target_page += 1
            offset_in_page = 0
            current_page = await self._fetch_data_page_async(
                self.root_model,
                query,
                page=target_page,
                page_size=page_size,
                sort_by=root_sort,
            )
            self._cache_records(model_cache, self.root_model, current_page.items)
        return items[:take_count]

    def _validate(self) -> None:
        for view_filter in self.filters:
            if view_filter.filter_type not in view_filter.source.filters:
                raise ValueError(
                    f"Filter {view_filter.filter_type.__name__} is not allowed on {view_filter.source.qualified_name}"
                )
        for sortable in self.sortables:
            if not sortable.source.sortable:
                raise ValueError(f"Field {sortable.source.qualified_name} is not sortable")

    def _empty_page(self, page: int, page_size: int) -> Page:
        return Page(items=[], page=page, page_size=page_size, total_items=0, total_pages=0)

    def _model_for_field(self, field_definition: FieldDefinition) -> type[BaseModel]:
        if field_definition.owner is None:
            raise ValueError("View source field is not bound to a model")
        return field_definition.owner

    def _field_name(self, field_definition: FieldDefinition) -> str:
        return field_definition.bound_name

    def _normalize_pagination(self, page: int, page_size: int | None) -> tuple[int, int]:
        if page < 1:
            raise ValueError("page must be at least 1")
        resolved_page_size = page_size or self.pagination.default_page_size
        if resolved_page_size < 1:
            raise ValueError("page_size must be at least 1")
        if resolved_page_size > self.pagination.max_page_size:
            raise ValueError("page_size exceeds the configured maximum")
        return page, resolved_page_size

    def _normalize_sort_input(
        self,
        sort_by: SortByInput,
    ) -> tuple[str, ...]:
        if sort_by is None:
            defaults = [
                sortable.name if sortable.default_sort == "asc" else f"-{sortable.name}"
                for sortable in self.sortables
                if sortable.default_sort
            ]
            return tuple(defaults[:1])
        if isinstance(sort_by, str):
            return (sort_by,)
        return tuple(sort_by)

    def _resolve_sort(self, sort_by: SortByInput) -> tuple[ViewSortable | None, bool]:
        resolved = self._resolve_sorts(sort_by)
        if not resolved:
            return None, False
        return resolved[0]

    def _resolve_sorts(
        self,
        sort_by: SortByInput,
    ) -> tuple[tuple[ViewSortable, bool], ...]:
        requested = self._normalize_sort_input(sort_by)
        resolved: builtins.list[tuple[ViewSortable, bool]] = []
        for requested_sort in requested:
            descending = requested_sort.startswith("-")
            sortable_name = requested_sort[1:] if descending else requested_sort
            try:
                resolved.append((self._sortables_by_name[sortable_name], descending))
            except KeyError as exc:
                raise ValueError(f"Unknown sort '{sortable_name}'") from exc
        return tuple(resolved)

    def _group_requested_filters(
        self,
        requested_filters: dict[str, Any],
    ) -> dict[type[BaseModel], builtins.list[BoundFilter]]:
        grouped: dict[type[BaseModel], builtins.list[BoundFilter]] = {}
        for filter_name, filter_value in requested_filters.items():
            try:
                view_filter = self._filters_by_name[filter_name]
            except KeyError as exc:
                raise ValueError(f"Unknown filter '{filter_name}'") from exc
            model = self._model_for_field(view_filter.source)
            grouped.setdefault(model, []).append(
                BoundFilter(
                    source=view_filter.source,
                    filter_type=view_filter.filter_type,
                    value=filter_value,
                )
            )
        return grouped

    def _prepare_root_query(
        self,
        grouped_filters: dict[type[BaseModel], builtins.list[BoundFilter]],
    ) -> PreparedRootQuery:
        root_filters = list(grouped_filters.get(self.root_model, ()))
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]] = {}

        for model, bound_filters in grouped_filters.items():
            if model is self.root_model:
                continue
            target_page = self._collect_all_records(
                model,
                ModelQuery.from_bound_filters(bound_filters),
                model_cache=model_cache,
            )
            if not target_page.items:
                return PreparedRootQuery(
                    query=ModelQuery(), model_cache=model_cache, impossible=True
                )
            propagated = self._propagate_filter_to_root(
                self._find_path(self.root_model, model),
                target_page.items,
                model_cache,
            )
            if propagated is None:
                return PreparedRootQuery(
                    query=ModelQuery(), model_cache=model_cache, impossible=True
                )
            root_filters.append(propagated)

        return PreparedRootQuery(
            query=ModelQuery.from_bound_filters(root_filters),
            model_cache=model_cache,
        )

    async def _prepare_root_query_async(
        self,
        grouped_filters: dict[type[BaseModel], builtins.list[BoundFilter]],
    ) -> PreparedRootQuery:
        root_filters = list(grouped_filters.get(self.root_model, ()))
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]] = {}

        for model, bound_filters in grouped_filters.items():
            if model is self.root_model:
                continue
            target_page = await self._collect_all_records_async(
                model,
                ModelQuery.from_bound_filters(bound_filters),
                model_cache=model_cache,
            )
            if not target_page.items:
                return PreparedRootQuery(
                    query=ModelQuery(), model_cache=model_cache, impossible=True
                )
            propagated = await self._propagate_filter_to_root_async(
                self._find_path(self.root_model, model),
                target_page.items,
                model_cache,
            )
            if propagated is None:
                return PreparedRootQuery(
                    query=ModelQuery(), model_cache=model_cache, impossible=True
                )
            root_filters.append(propagated)

        return PreparedRootQuery(
            query=ModelQuery.from_bound_filters(root_filters),
            model_cache=model_cache,
        )

    def _propagate_filter_to_root(
        self,
        path: tuple[RelationStep, ...],
        matching_target_records: builtins.list[BaseModel],
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> BoundFilter | None:
        current_records = matching_target_records
        for step in reversed(path):
            join_values = {getattr(record, step.next_field_name) for record in current_records}
            if not join_values:
                return None
            if step.current_model is self.root_model:
                return BoundFilter(
                    source=self.root_model.field(step.current_field_name),
                    filter_type=InListFilter,
                    value=join_values,
                )
            current_records = self._collect_all_records(
                step.current_model,
                ModelQuery.from_bound_filters(
                    (
                        BoundFilter(
                            source=step.current_model.field(step.current_field_name),
                            filter_type=InListFilter,
                            value=join_values,
                        ),
                    )
                ),
                model_cache=model_cache,
            ).items
        return None

    async def _propagate_filter_to_root_async(
        self,
        path: tuple[RelationStep, ...],
        matching_target_records: builtins.list[BaseModel],
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> BoundFilter | None:
        current_records = matching_target_records
        for step in reversed(path):
            join_values = {getattr(record, step.next_field_name) for record in current_records}
            if not join_values:
                return None
            if step.current_model is self.root_model:
                return BoundFilter(
                    source=self.root_model.field(step.current_field_name),
                    filter_type=InListFilter,
                    value=join_values,
                )
            current_records = (
                await self._collect_all_records_async(
                    step.current_model,
                    ModelQuery.from_bound_filters(
                        (
                            BoundFilter(
                                source=step.current_model.field(step.current_field_name),
                                filter_type=InListFilter,
                                value=join_values,
                            ),
                        )
                    ),
                    model_cache=model_cache,
                )
            ).items
        return None

    def _can_page_root(self, sortable: ViewSortable | None) -> bool:
        return self._can_page_root_source(sortable.source if sortable is not None else None)

    def _can_page_root_source(self, sort_source: FieldDefinition | None) -> bool:
        return self._can_page_root_sources((sort_source,) if sort_source is not None else ())

    def _can_page_root_sources(self, sort_sources: tuple[FieldDefinition, ...]) -> bool:
        if not self.root_model.datasource.paginated:
            return False
        if not sort_sources:
            return True
        return all(
            self._model_for_field(sort_source) is self.root_model for sort_source in sort_sources
        )

    def _can_grouped_root_page(
        self,
        resolved_sorts: tuple[tuple[ViewSortable, bool], ...],
    ) -> bool:
        if len(resolved_sorts) < 2:
            return False
        first_source = resolved_sorts[0][0].source
        if self._model_for_field(first_source) is self.root_model:
            return False
        if any(
            self._model_for_field(sortable.source) is not self.root_model
            for sortable, _ in resolved_sorts[1:]
        ):
            return False
        path = self._find_path(self.root_model, self._model_for_field(first_source))
        return len(path) == 1 and path[0].current_model is self.root_model

    def _root_sort(self, sortable: ViewSortable | None, descending: bool) -> str | None:
        return self._root_sort_from_source(
            sortable.source if sortable is not None else None, descending
        )

    def _root_sort_from_source(
        self, sort_source: FieldDefinition | None, descending: bool
    ) -> str | None:
        if sort_source is None:
            return None
        if self._model_for_field(sort_source) is not self.root_model:
            return None
        prefix = "-" if descending else ""
        return f"{prefix}{self._field_name(sort_source)}"

    def _serialize_root_sorts(
        self,
        resolved_sorts: tuple[tuple[ViewSortable, bool], ...],
    ) -> tuple[str, ...]:
        serialized: builtins.list[str] = []
        for sortable, descending in resolved_sorts:
            root_sort = self._root_sort_from_source(sortable.source, descending)
            if root_sort is None:
                continue
            serialized.append(root_sort)
        return tuple(serialized)

    def _serialize_root_sorts_for_step(
        self,
        resolved_sorts: (
            builtins.list[tuple[ViewSortable, bool]] | tuple[tuple[ViewSortable, bool], ...]
        ),
    ) -> str | None:
        values = [
            ("-" if descending else "") + self._field_name(sortable.source)
            for sortable, descending in resolved_sorts
        ]
        if not values:
            return None
        if len(values) == 1:
            return values[0]
        return repr(values)

    def _sort_by_argument(self, sort_values: tuple[str, ...]) -> str | tuple[str, ...] | None:
        if not sort_values:
            return None
        if len(sort_values) == 1:
            return sort_values[0]
        return sort_values

    def _root_field_for_path(self, path: tuple[RelationStep, ...]) -> str:
        for step in reversed(path):
            if step.current_model is self.root_model:
                return step.current_field_name
        raise ValueError("Relation path does not resolve back to the root model")

    def _path_descriptions(self, path: tuple[RelationStep, ...]) -> tuple[str, ...]:
        return tuple(
            f"{step.current_model.__name__}.{step.current_field_name}->{step.next_model.__name__}.{step.next_field_name}"
            for step in path
        )

    def _sort_root_records(
        self,
        root_records: builtins.list[BaseModel],
        sortable: ViewSortable | None,
        descending: bool,
        path_cache: PathCache,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> builtins.list[BaseModel]:
        return self._sort_root_records_by_source(
            root_records,
            (sortable.source,) if sortable is not None else (),
            (descending,) if sortable is not None else (),
            path_cache,
            model_cache,
        )

    def _sort_root_records_by_source(
        self,
        root_records: builtins.list[BaseModel],
        sort_sources: tuple[FieldDefinition, ...],
        descending_flags: tuple[bool, ...],
        path_cache: PathCache,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> builtins.list[BaseModel]:
        for sort_source, descending in reversed(
            tuple(zip(sort_sources, descending_flags, strict=True))
        ):
            source_model = self._model_for_field(sort_source)
            if source_model is self.root_model:
                root_records.sort(
                    key=lambda record: self._coerce_sort_value(
                        getattr(record, self._field_name(sort_source))
                    ),
                    reverse=descending,
                )
                continue

            root_records.sort(
                key=lambda record: self._coerce_sort_value(
                    self._resolve_source_value(
                        record,
                        sort_source,
                        root_records,
                        path_cache,
                        model_cache,
                    )
                ),
                reverse=descending,
            )
        return root_records

    async def _sort_root_records_async(
        self,
        root_records: builtins.list[BaseModel],
        sortable: ViewSortable | None,
        descending: bool,
        path_cache: PathCache,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> builtins.list[BaseModel]:
        return await self._sort_root_records_by_source_async(
            root_records,
            (sortable.source,) if sortable is not None else (),
            (descending,) if sortable is not None else (),
            path_cache,
            model_cache,
        )

    async def _sort_root_records_by_source_async(
        self,
        root_records: builtins.list[BaseModel],
        sort_sources: tuple[FieldDefinition, ...],
        descending_flags: tuple[bool, ...],
        path_cache: PathCache,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> builtins.list[BaseModel]:
        for sort_source, descending in reversed(
            tuple(zip(sort_sources, descending_flags, strict=True))
        ):
            source_model = self._model_for_field(sort_source)
            if source_model is self.root_model:
                root_records.sort(
                    key=lambda record: self._coerce_sort_value(
                        getattr(record, self._field_name(sort_source))
                    ),
                    reverse=descending,
                )
                continue

            sort_pairs = []
            for record in root_records:
                sort_value = await self._resolve_source_value_async(
                    record,
                    sort_source,
                    root_records,
                    path_cache,
                    model_cache,
                )
                sort_pairs.append((record, self._coerce_sort_value(sort_value)))
            root_records = [
                record
                for record, _ in sorted(sort_pairs, key=lambda pair: pair[1], reverse=descending)
            ]
        return root_records

    def _resolve_source_value(
        self,
        root_record: BaseModel,
        source: FieldDefinition,
        root_records: builtins.list[BaseModel],
        path_cache: PathCache,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> Any:
        source_model = self._model_for_field(source)
        if source_model is self.root_model:
            return getattr(root_record, self._field_name(source))

        path = self._find_path(self.root_model, source_model)
        indexes = self._load_path_indexes(path, root_records, path_cache, model_cache)
        current_records = [root_record]
        for step, index in zip(path, indexes, strict=True):
            next_records: builtins.list[BaseModel] = []
            for record in current_records:
                next_records.extend(index.get(getattr(record, step.current_field_name), ()))
            current_records = self._deduplicate_records(step.next_model, next_records)
            if not current_records:
                return None

        values = [getattr(record, self._field_name(source)) for record in current_records]
        if len(values) == 1:
            return values[0]
        return values

    async def _resolve_source_value_async(
        self,
        root_record: BaseModel,
        source: FieldDefinition,
        root_records: builtins.list[BaseModel],
        path_cache: PathCache,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> Any:
        source_model = self._model_for_field(source)
        if source_model is self.root_model:
            return getattr(root_record, self._field_name(source))

        path = self._find_path(self.root_model, source_model)
        indexes = await self._load_path_indexes_async(path, root_records, path_cache, model_cache)
        current_records = [root_record]
        for step, index in zip(path, indexes, strict=True):
            next_records: builtins.list[BaseModel] = []
            for record in current_records:
                next_records.extend(index.get(getattr(record, step.current_field_name), ()))
            current_records = self._deduplicate_records(step.next_model, next_records)
            if not current_records:
                return None

        values = [getattr(record, self._field_name(source)) for record in current_records]
        if len(values) == 1:
            return values[0]
        return values

    def _coerce_sort_value(self, value: Any) -> Any:
        if isinstance(value, list):
            raise ValueError("Sorting by a multi-valued relation is not supported")
        return (value is None, value)

    def _load_path_indexes(
        self,
        path: tuple[RelationStep, ...],
        root_records: builtins.list[BaseModel],
        path_cache: PathCache,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> builtins.list[PathIndex]:
        cached = path_cache.get(path)
        if cached is not None:
            return cached

        indexes: builtins.list[PathIndex] = []
        current_records = root_records
        for step in path:
            join_values = {getattr(record, step.current_field_name) for record in current_records}
            next_records = self._records_for_join(step, join_values, model_cache)
            if next_records is None:
                next_records = self._collect_all_records(
                    step.next_model,
                    ModelQuery.from_bound_filters(
                        (
                            BoundFilter(
                                source=step.next_model.field(step.next_field_name),
                                filter_type=InListFilter,
                                value=join_values,
                            ),
                        )
                    ),
                    model_cache=model_cache,
                ).items
            index: PathIndex = {}
            for record in next_records:
                index.setdefault(getattr(record, step.next_field_name), []).append(record)
            indexes.append(index)
            current_records = next_records

        path_cache[path] = indexes
        return indexes

    async def _load_path_indexes_async(
        self,
        path: tuple[RelationStep, ...],
        root_records: builtins.list[BaseModel],
        path_cache: PathCache,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> builtins.list[PathIndex]:
        cached = path_cache.get(path)
        if cached is not None:
            return cached

        indexes: builtins.list[PathIndex] = []
        current_records = root_records
        for step in path:
            join_values = {getattr(record, step.current_field_name) for record in current_records}
            next_records = self._records_for_join(step, join_values, model_cache)
            if next_records is None:
                next_records = (
                    await self._collect_all_records_async(
                        step.next_model,
                        ModelQuery.from_bound_filters(
                            (
                                BoundFilter(
                                    source=step.next_model.field(step.next_field_name),
                                    filter_type=InListFilter,
                                    value=join_values,
                                ),
                            )
                        ),
                        model_cache=model_cache,
                    )
                ).items
            index: PathIndex = {}
            for record in next_records:
                index.setdefault(getattr(record, step.next_field_name), []).append(record)
            indexes.append(index)
            current_records = next_records

        path_cache[path] = indexes
        return indexes

    def _records_for_join(
        self,
        step: RelationStep,
        join_values: set[Any],
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> builtins.list[BaseModel] | None:
        if not join_values:
            return []
        pk_name = step.next_model.primary_key_field().bound_name
        if step.next_field_name != pk_name:
            return None
        cached_records = model_cache.get(step.next_model, {})
        if not join_values.issubset(cached_records):
            return None
        return [cached_records[value] for value in join_values]

    def _collect_all_records(
        self,
        model: type[BaseModel],
        query: ModelQuery,
        *,
        sort_by: str | tuple[str, ...] | None = None,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> DataPage[BaseModel]:
        collection_page_size = model.datasource.max_page_size or model.datasource.default_page_size
        first_page = self._fetch_data_page(
            model,
            query,
            page=1,
            page_size=collection_page_size,
            sort_by=sort_by,
        )
        items = list(first_page.items)
        self._cache_records(model_cache, model, first_page.items)
        if not model.datasource.paginated or first_page.total_pages <= 1:
            return DataPage(
                items=model.normalize_records(items),
                page=1,
                page_size=first_page.page_size,
                total_items=first_page.total_items,
                total_pages=first_page.total_pages,
            )

        for page_number in range(2, first_page.total_pages + 1):
            next_page = self._fetch_data_page(
                model,
                query,
                page=page_number,
                page_size=first_page.page_size,
                sort_by=sort_by,
            )
            items.extend(next_page.items)
            self._cache_records(model_cache, model, next_page.items)

        return DataPage(
            items=model.normalize_records(items),
            page=1,
            page_size=first_page.page_size,
            total_items=first_page.total_items,
            total_pages=first_page.total_pages,
        )

    async def _collect_all_records_async(
        self,
        model: type[BaseModel],
        query: ModelQuery,
        *,
        sort_by: str | tuple[str, ...] | None = None,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> DataPage[BaseModel]:
        collection_page_size = model.datasource.max_page_size or model.datasource.default_page_size
        first_page = await self._fetch_data_page_async(
            model,
            query,
            page=1,
            page_size=collection_page_size,
            sort_by=sort_by,
        )
        items = list(first_page.items)
        self._cache_records(model_cache, model, first_page.items)
        if not model.datasource.paginated or first_page.total_pages <= 1:
            return DataPage(
                items=model.normalize_records(items),
                page=1,
                page_size=first_page.page_size,
                total_items=first_page.total_items,
                total_pages=first_page.total_pages,
            )

        for page_number in range(2, first_page.total_pages + 1):
            next_page = await self._fetch_data_page_async(
                model,
                query,
                page=page_number,
                page_size=first_page.page_size,
                sort_by=sort_by,
            )
            items.extend(next_page.items)
            self._cache_records(model_cache, model, next_page.items)

        return DataPage(
            items=model.normalize_records(items),
            page=1,
            page_size=first_page.page_size,
            total_items=first_page.total_items,
            total_pages=first_page.total_pages,
        )

    def _cache_records(
        self,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
        model: type[BaseModel],
        records: builtins.list[BaseModel],
    ) -> None:
        pk_name = model.primary_key_field().bound_name
        cache = model_cache.setdefault(model, {})
        for record in records:
            cache[getattr(record, pk_name)] = record

    def _find_path(
        self,
        start: type[BaseModel],
        target: type[BaseModel],
    ) -> tuple[RelationStep, ...]:
        if start is target:
            return ()

        queue: deque[tuple[type[BaseModel], tuple[RelationStep, ...]]] = deque([(start, ())])
        visited = {start}

        while queue:
            model, path = queue.popleft()
            for step in self._relation_steps_from(model):
                if step.next_model in visited:
                    continue
                next_path = path + (step,)
                if step.next_model is target:
                    return next_path
                visited.add(step.next_model)
                queue.append((step.next_model, next_path))

        raise ValueError(f"No relation path from {start.__name__} to {target.__name__}")

    def _relation_steps_from(self, model: type[BaseModel]) -> builtins.list[RelationStep]:
        steps: builtins.list[RelationStep] = []
        for relation_name, relation in model.__relations__.items():
            steps.append(self._build_step(model, relation_name, relation, forward=True))

        for candidate_model in BaseModel._registry.values():
            for relation_name, relation in candidate_model.__relations__.items():
                if relation.resolve_target() is model:
                    steps.append(
                        self._build_step(candidate_model, relation_name, relation, forward=False)
                    )

        return steps

    def _build_step(
        self,
        source_model: type[BaseModel],
        relation_name: str,
        relation: RelationDefinition,
        *,
        forward: bool,
    ) -> RelationStep:
        target_model = relation.resolve_target()
        source_pk_name = source_model.primary_key_field().bound_name
        target_pk_name = target_model.primary_key_field().bound_name

        if relation.relation_type in {"many-to-one", "one-to-one"}:
            current_field_name = relation.foreign_key
            next_field_name = target_pk_name
        elif relation.relation_type == "one-to-many":
            current_field_name = source_pk_name
            next_field_name = relation.foreign_key
        else:
            raise ValueError(f"Unsupported relation type '{relation.relation_type}'")

        if forward:
            return RelationStep(
                current_model=source_model,
                next_model=target_model,
                current_field_name=current_field_name,
                next_field_name=next_field_name,
                relation_name=relation_name,
            )

        return RelationStep(
            current_model=target_model,
            next_model=source_model,
            current_field_name=next_field_name,
            next_field_name=current_field_name,
            relation_name=relation_name,
        )

    def _fetch_data_page(
        self,
        model: type[BaseModel],
        query: ModelQuery,
        *,
        page: int,
        page_size: int | None,
        sort_by: str | tuple[str, ...] | None,
    ) -> DataPage[BaseModel]:
        return model.normalize_page(
            model.list(query, page=page, page_size=page_size, sort_by=sort_by)
        )

    async def _fetch_data_page_async(
        self,
        model: type[BaseModel],
        query: ModelQuery,
        *,
        page: int,
        page_size: int | None,
        sort_by: str | tuple[str, ...] | None,
    ) -> DataPage[BaseModel]:
        records_or_awaitable = model.list_async(
            query,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
        )
        records: DataPage[Any] | Sequence[Any]
        if inspect.isawaitable(records_or_awaitable):
            try:
                records = await cast(
                    Awaitable[DataPage[Any] | Sequence[Any]],
                    records_or_awaitable,
                )
            except NotImplementedError:
                return self._fetch_data_page(
                    model, query, page=page, page_size=page_size, sort_by=sort_by
                )
        else:
            records = records_or_awaitable
        return model.normalize_page(records)

    def _deduplicate_records(
        self,
        model: type[BaseModel],
        records: builtins.list[BaseModel],
    ) -> builtins.list[BaseModel]:
        pk_name = model.primary_key_field().bound_name
        deduplicated: dict[Any, BaseModel] = {}
        for record in records:
            deduplicated[getattr(record, pk_name)] = record
        return list(deduplicated.values())


def create_view(
    *,
    fields: builtins.list[ViewField],
    filters: builtins.list[ViewFilter] | None = None,
    sortables: builtins.list[ViewSortable] | None = None,
    pagination: PaginationConfig | None = None,
) -> View:
    return View(fields=fields, filters=filters, sortables=sortables, pagination=pagination)
