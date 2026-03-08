from __future__ import annotations

import builtins
import math
from typing import Any

from ..datasource import DataPage
from ..filters import EqualsFilter
from ..models import BaseModel
from ..plans import ViewExecutionPlan
from ..query import BoundFilter, ModelQuery
from .support import ViewSupport
from .types import Page, PathCache, PreparedRootQuery, RelationStep


class ViewExecutionMixin(ViewSupport):
    def _execute_plan(self, plan: ViewExecutionPlan) -> Page:
        prepared = self._prepare_root_query(plan.grouped_filter_map())
        if prepared.impossible:
            return self._empty_page(plan.request.page, plan.request.page_size)

        if plan.execution_mode == "root_page":
            return self._execute_root_page(plan, prepared)
        if plan.execution_mode == "grouped_root_page":
            return self._execute_grouped_plan(plan, prepared)
        return self._execute_collected_page(plan, prepared)

    async def _execute_plan_async(self, plan: ViewExecutionPlan) -> Page:
        prepared = await self._prepare_root_query_async(plan.grouped_filter_map())
        if prepared.impossible:
            return self._empty_page(plan.request.page, plan.request.page_size)

        if plan.execution_mode == "root_page":
            return await self._execute_root_page_async(plan, prepared)
        if plan.execution_mode == "grouped_root_page":
            return await self._execute_grouped_plan_async(plan, prepared)
        return await self._execute_collected_page_async(plan, prepared)

    def _execute_root_page(self, plan: ViewExecutionPlan, prepared: PreparedRootQuery) -> Page:
        root_page = self._fetch_data_page(
            self.root_model,
            prepared.query,
            page=plan.request.page,
            page_size=plan.request.page_size,
            sort_by=self._sort_by_argument(plan.root_sort_by),
        )
        self._cache_records(prepared.model_cache, self.root_model, root_page.items)
        items = self._project_records(root_page.items, root_page.items, prepared.model_cache)
        return self._build_page(
            items=items,
            page=plan.request.page,
            page_size=plan.request.page_size,
            total_items=root_page.total_items,
        )

    async def _execute_root_page_async(
        self,
        plan: ViewExecutionPlan,
        prepared: PreparedRootQuery,
    ) -> Page:
        root_page = await self._fetch_data_page_async(
            self.root_model,
            prepared.query,
            page=plan.request.page,
            page_size=plan.request.page_size,
            sort_by=self._sort_by_argument(plan.root_sort_by),
        )
        self._cache_records(prepared.model_cache, self.root_model, root_page.items)
        items = await self._project_records_async(
            root_page.items,
            root_page.items,
            prepared.model_cache,
        )
        return self._build_page(
            items=items,
            page=plan.request.page,
            page_size=plan.request.page_size,
            total_items=root_page.total_items,
        )

    def _execute_collected_page(
        self,
        plan: ViewExecutionPlan,
        prepared: PreparedRootQuery,
    ) -> Page:
        path_cache: PathCache = {}
        collected_root_page = self._collect_all_records(
            self.root_model,
            prepared.query,
            sort_by=None,
            model_cache=prepared.model_cache,
        )
        root_records = self._sort_root_records_by_source(
            collected_root_page.items,
            plan.sort_sources,
            plan.descending_flags,
            path_cache,
            prepared.model_cache,
        )
        paged_records = self._slice_page(
            root_records,
            page=plan.request.page,
            page_size=plan.request.page_size,
        )
        items = self._project_records(
            paged_records,
            root_records,
            prepared.model_cache,
            path_cache,
        )
        return self._build_page(
            items=items,
            page=plan.request.page,
            page_size=plan.request.page_size,
            total_items=len(root_records),
        )

    async def _execute_collected_page_async(
        self,
        plan: ViewExecutionPlan,
        prepared: PreparedRootQuery,
    ) -> Page:
        path_cache: PathCache = {}
        collected_root_page = await self._collect_all_records_async(
            self.root_model,
            prepared.query,
            sort_by=None,
            model_cache=prepared.model_cache,
        )
        root_records = await self._sort_root_records_by_source_async(
            collected_root_page.items,
            plan.sort_sources,
            plan.descending_flags,
            path_cache,
            prepared.model_cache,
        )
        paged_records = self._slice_page(
            root_records,
            page=plan.request.page,
            page_size=plan.request.page_size,
        )
        items = await self._project_records_async(
            paged_records,
            root_records,
            prepared.model_cache,
            path_cache,
        )
        return self._build_page(
            items=items,
            page=plan.request.page,
            page_size=plan.request.page_size,
            total_items=len(root_records),
        )

    def _execute_grouped_plan(
        self,
        plan: ViewExecutionPlan,
        prepared: PreparedRootQuery,
    ) -> Page:
        group_source = plan.sort_sources[0]
        group_model = self._model_for_field(group_source)
        group_query = ModelQuery.from_bound_filters(plan.grouped_filter_map().get(group_model, ()))
        group_sort = ("-" if plan.descending_flags[0] else "") + self._field_name(group_source)
        group_page_size = (
            group_model.datasource.max_page_size or group_model.datasource.default_page_size
        )
        root_sort = self._sort_by_argument(plan.root_sort_by)
        selected_records: builtins.list[BaseModel] = []
        total_items = 0
        offset = (plan.request.page - 1) * plan.request.page_size
        needed = plan.request.page_size

        join_step, group_pk_name = self._group_join_context(group_model)

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
            self._cache_records(prepared.model_cache, group_model, group_page.items)

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
                    model_cache=prepared.model_cache,
                )
                total_items += group_total
                if group_records:
                    selected_records.extend(group_records)
                    needed -= len(group_records)

            group_page_number += 1

        items = self._project_records(
            selected_records,
            selected_records,
            prepared.model_cache,
        )
        return self._build_page(
            items=items,
            page=plan.request.page,
            page_size=plan.request.page_size,
            total_items=total_items,
        )

    async def _execute_grouped_plan_async(
        self,
        plan: ViewExecutionPlan,
        prepared: PreparedRootQuery,
    ) -> Page:
        group_source = plan.sort_sources[0]
        group_model = self._model_for_field(group_source)
        group_query = ModelQuery.from_bound_filters(plan.grouped_filter_map().get(group_model, ()))
        group_sort = ("-" if plan.descending_flags[0] else "") + self._field_name(group_source)
        group_page_size = (
            group_model.datasource.max_page_size or group_model.datasource.default_page_size
        )
        root_sort = self._sort_by_argument(plan.root_sort_by)
        selected_records: builtins.list[BaseModel] = []
        total_items = 0
        offset = (plan.request.page - 1) * plan.request.page_size
        needed = plan.request.page_size

        join_step, group_pk_name = self._group_join_context(group_model)

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
            self._cache_records(prepared.model_cache, group_model, group_page.items)

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
                    model_cache=prepared.model_cache,
                )
                total_items += group_total
                if group_records:
                    selected_records.extend(group_records)
                    needed -= len(group_records)

            group_page_number += 1

        items = await self._project_records_async(
            selected_records,
            selected_records,
            prepared.model_cache,
        )
        return self._build_page(
            items=items,
            page=plan.request.page,
            page_size=plan.request.page_size,
            total_items=total_items,
        )

    def _group_join_context(self, group_model: type[BaseModel]) -> tuple[RelationStep, str]:
        first_path = self._find_path(self.root_model, group_model)
        if len(first_path) != 1:
            raise ValueError("Grouped root execution requires a direct relation path")
        return first_path[0], group_model.primary_key_field().bound_name

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
        grouped_query = self._append_bound_filters(
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
            grouped_query,
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
                query=grouped_query,
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
        grouped_query = self._append_bound_filters(
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
            grouped_query,
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
                query=grouped_query,
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
        if target_page != seed_page.page:
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
        if target_page != seed_page.page:
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

    def _project_records(
        self,
        records: builtins.list[BaseModel],
        root_records: builtins.list[BaseModel],
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
        path_cache: PathCache | None = None,
    ) -> builtins.list[dict[str, Any]]:
        resolved_path_cache = path_cache or {}
        return [
            {
                field.name: self._resolve_source_value(
                    record,
                    field.source,
                    root_records,
                    resolved_path_cache,
                    model_cache,
                )
                for field in self.fields
            }
            for record in records
        ]

    async def _project_records_async(
        self,
        records: builtins.list[BaseModel],
        root_records: builtins.list[BaseModel],
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
        path_cache: PathCache | None = None,
    ) -> builtins.list[dict[str, Any]]:
        resolved_path_cache = path_cache or {}
        items: builtins.list[dict[str, Any]] = []
        for record in records:
            item = {
                field.name: await self._resolve_source_value_async(
                    record,
                    field.source,
                    root_records,
                    resolved_path_cache,
                    model_cache,
                )
                for field in self.fields
            }
            items.append(item)
        return items

    def _slice_page(
        self,
        records: builtins.list[BaseModel],
        *,
        page: int,
        page_size: int,
    ) -> builtins.list[BaseModel]:
        start = (page - 1) * page_size
        end = start + page_size
        return records[start:end]

    def _build_page(
        self,
        *,
        items: builtins.list[dict[str, Any]],
        page: int,
        page_size: int,
        total_items: int,
    ) -> Page:
        total_pages = math.ceil(total_items / page_size) if total_items else 0
        return Page(
            items=items,
            page=page,
            page_size=page_size,
            total_items=total_items,
            total_pages=total_pages,
        )
