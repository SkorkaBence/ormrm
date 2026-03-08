from __future__ import annotations

import builtins
from typing import Any

from ..datasource import DataPage
from ..models import BaseModel
from ..plans import ViewExecutionPlan
from ..query import BoundFilter, ModelQuery
from ..schema import FieldDefinition, RelationDefinition

from .types import (
    Page,
    PaginationConfig,
    PathCache,
    PathIndex,
    PreparedRootQuery,
    RelationStep,
    SortByInput,
    ViewField,
    ViewFilter,
    ViewSortable,
)


class ViewSupport:
    fields: builtins.list[ViewField]
    filters: builtins.list[ViewFilter]
    sortables: builtins.list[ViewSortable]
    pagination: PaginationConfig
    root_model: type[BaseModel]
    _filters_by_name: dict[str, ViewFilter]
    _sortables_by_name: dict[str, ViewSortable]

    def _empty_page(self, page: int, page_size: int) -> Page:
        raise NotImplementedError

    def _prepare_root_query(
        self,
        grouped_filters: dict[type[BaseModel], builtins.list[BoundFilter]],
    ) -> PreparedRootQuery:
        raise NotImplementedError

    async def _prepare_root_query_async(
        self,
        grouped_filters: dict[type[BaseModel], builtins.list[BoundFilter]],
    ) -> PreparedRootQuery:
        raise NotImplementedError

    def _collect_all_records(
        self,
        model: type[BaseModel],
        query: ModelQuery,
        *,
        sort_by: str | tuple[str, ...] | None = None,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> DataPage[BaseModel]:
        raise NotImplementedError

    async def _collect_all_records_async(
        self,
        model: type[BaseModel],
        query: ModelQuery,
        *,
        sort_by: str | tuple[str, ...] | None = None,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> DataPage[BaseModel]:
        raise NotImplementedError

    def _find_path(
        self,
        start: type[BaseModel],
        target: type[BaseModel],
    ) -> tuple[RelationStep, ...]:
        raise NotImplementedError

    def _sort_root_records_by_source(
        self,
        root_records: builtins.list[BaseModel],
        sort_sources: tuple[FieldDefinition, ...],
        descending_flags: tuple[bool, ...],
        path_cache: PathCache,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> builtins.list[BaseModel]:
        raise NotImplementedError

    async def _sort_root_records_by_source_async(
        self,
        root_records: builtins.list[BaseModel],
        sort_sources: tuple[FieldDefinition, ...],
        descending_flags: tuple[bool, ...],
        path_cache: PathCache,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> builtins.list[BaseModel]:
        raise NotImplementedError

    def _resolve_source_value(
        self,
        root_record: BaseModel,
        source: FieldDefinition,
        root_records: builtins.list[BaseModel],
        path_cache: PathCache,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> Any:
        raise NotImplementedError

    async def _resolve_source_value_async(
        self,
        root_record: BaseModel,
        source: FieldDefinition,
        root_records: builtins.list[BaseModel],
        path_cache: PathCache,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> Any:
        raise NotImplementedError

    def _fetch_data_page(
        self,
        model: type[BaseModel],
        query: ModelQuery,
        *,
        page: int,
        page_size: int | None,
        sort_by: str | tuple[str, ...] | None,
    ) -> DataPage[BaseModel]:
        raise NotImplementedError

    async def _fetch_data_page_async(
        self,
        model: type[BaseModel],
        query: ModelQuery,
        *,
        page: int,
        page_size: int | None,
        sort_by: str | tuple[str, ...] | None,
    ) -> DataPage[BaseModel]:
        raise NotImplementedError

    def _cache_records(
        self,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
        model: type[BaseModel],
        records: builtins.list[BaseModel],
    ) -> None:
        raise NotImplementedError

    def _sort_by_argument(self, sort_values: tuple[str, ...]) -> str | tuple[str, ...] | None:
        raise NotImplementedError

    def _model_for_field(self, field_definition: FieldDefinition) -> type[BaseModel]:
        raise NotImplementedError

    def _field_name(self, field_definition: FieldDefinition) -> str:
        raise NotImplementedError

    def _group_join_context(self, group_model: type[BaseModel]) -> tuple[RelationStep, str]:
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

    def _append_bound_filters(
        self,
        query: ModelQuery,
        extra_filters: tuple[BoundFilter, ...],
    ) -> ModelQuery:
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

    def _project_records(
        self,
        records: builtins.list[BaseModel],
        root_records: builtins.list[BaseModel],
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
        path_cache: PathCache | None = None,
    ) -> builtins.list[dict[str, Any]]:
        raise NotImplementedError

    async def _project_records_async(
        self,
        records: builtins.list[BaseModel],
        root_records: builtins.list[BaseModel],
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
        path_cache: PathCache | None = None,
    ) -> builtins.list[dict[str, Any]]:
        raise NotImplementedError

    def _slice_page(
        self,
        records: builtins.list[BaseModel],
        *,
        page: int,
        page_size: int,
    ) -> builtins.list[BaseModel]:
        raise NotImplementedError

    def _build_page(
        self,
        *,
        items: builtins.list[dict[str, Any]],
        page: int,
        page_size: int,
        total_items: int,
    ) -> Page:
        raise NotImplementedError

    def _normalize_pagination(self, page: int, page_size: int | None) -> tuple[int, int]:
        raise NotImplementedError

    def _normalize_sort_input(self, sort_by: SortByInput) -> tuple[str, ...]:
        raise NotImplementedError

    def _resolve_sort(self, sort_by: SortByInput) -> tuple[ViewSortable | None, bool]:
        raise NotImplementedError

    def _resolve_sorts(
        self,
        sort_by: SortByInput,
    ) -> tuple[tuple[ViewSortable, bool], ...]:
        raise NotImplementedError

    def _group_requested_filters(
        self,
        requested_filters: dict[str, Any],
    ) -> dict[type[BaseModel], builtins.list[BoundFilter]]:
        raise NotImplementedError

    def _propagate_filter_to_root(
        self,
        path: tuple[RelationStep, ...],
        matching_target_records: builtins.list[BaseModel],
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> BoundFilter | None:
        raise NotImplementedError

    async def _propagate_filter_to_root_async(
        self,
        path: tuple[RelationStep, ...],
        matching_target_records: builtins.list[BaseModel],
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> BoundFilter | None:
        raise NotImplementedError

    def _can_page_root(self, sortable: ViewSortable | None) -> bool:
        raise NotImplementedError

    def _can_page_root_source(self, sort_source: FieldDefinition | None) -> bool:
        raise NotImplementedError

    def _can_page_root_sources(self, sort_sources: tuple[FieldDefinition, ...]) -> bool:
        raise NotImplementedError

    def _can_grouped_root_page(
        self,
        resolved_sorts: tuple[tuple[ViewSortable, bool], ...],
    ) -> bool:
        raise NotImplementedError

    def _root_sort(self, sortable: ViewSortable | None, descending: bool) -> str | None:
        raise NotImplementedError

    def _root_sort_from_source(
        self,
        sort_source: FieldDefinition | None,
        descending: bool,
    ) -> str | None:
        raise NotImplementedError

    def _serialize_root_sorts(
        self,
        resolved_sorts: tuple[tuple[ViewSortable, bool], ...],
    ) -> tuple[str, ...]:
        raise NotImplementedError

    def _serialize_root_sorts_for_step(
        self,
        resolved_sorts: (
            builtins.list[tuple[ViewSortable, bool]] | tuple[tuple[ViewSortable, bool], ...]
        ),
    ) -> str | None:
        raise NotImplementedError

    def _root_field_for_path(self, path: tuple[RelationStep, ...]) -> str:
        raise NotImplementedError

    def _path_descriptions(self, path: tuple[RelationStep, ...]) -> tuple[str, ...]:
        raise NotImplementedError

    def _records_for_join(
        self,
        step: RelationStep,
        join_values: set[Any],
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> builtins.list[BaseModel] | None:
        raise NotImplementedError

    def _rebuilt_collection_page(
        self,
        model: type[BaseModel],
        items: builtins.list[BaseModel],
        seed_page: DataPage[BaseModel],
    ) -> DataPage[BaseModel]:
        raise NotImplementedError

    def _relation_steps_from(self, model: type[BaseModel]) -> builtins.list[RelationStep]:
        raise NotImplementedError

    def _build_step(
        self,
        source_model: type[BaseModel],
        relation_name: str,
        relation: RelationDefinition,
        *,
        forward: bool,
    ) -> RelationStep:
        raise NotImplementedError

    def _deduplicate_records(
        self,
        model: type[BaseModel],
        records: builtins.list[BaseModel],
    ) -> builtins.list[BaseModel]:
        raise NotImplementedError

    def _load_path_indexes(
        self,
        path: tuple[RelationStep, ...],
        root_records: builtins.list[BaseModel],
        path_cache: PathCache,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> builtins.list[PathIndex]:
        raise NotImplementedError

    async def _load_path_indexes_async(
        self,
        path: tuple[RelationStep, ...],
        root_records: builtins.list[BaseModel],
        path_cache: PathCache,
        model_cache: dict[type[BaseModel], dict[Any, BaseModel]],
    ) -> builtins.list[PathIndex]:
        raise NotImplementedError

    def _coerce_sort_value(self, value: Any) -> Any:
        raise NotImplementedError

    def _execute_plan(self, plan: ViewExecutionPlan) -> Page:
        raise NotImplementedError

    async def _execute_plan_async(self, plan: ViewExecutionPlan) -> Page:
        raise NotImplementedError
