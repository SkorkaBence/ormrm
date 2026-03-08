from __future__ import annotations

import builtins
from typing import Any

from ..filters import InListFilter
from ..models import BaseModel
from ..query import BoundFilter, ModelQuery
from ..schema import FieldDefinition

from .support import ViewSupport
from .types import Page, PreparedRootQuery, RelationStep, SortByInput, ViewSortable


class ViewPlanningMixin(ViewSupport):
    def _validate(self) -> None:
        for view_filter in self.filters:
            if view_filter.filter_type not in view_filter.source.filters:
                raise ValueError(
                    f"Filter {view_filter.filter_type.__name__} is not allowed on "
                    f"{view_filter.source.qualified_name}"
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

    def _normalize_sort_input(self, sort_by: SortByInput) -> tuple[str, ...]:
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

    def _resolve_sorts(self, sort_by: SortByInput) -> tuple[tuple[ViewSortable, bool], ...]:
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
                    query=ModelQuery(),
                    model_cache=model_cache,
                    impossible=True,
                )

            propagated = self._propagate_filter_to_root(
                self._find_path(self.root_model, model),
                target_page.items,
                model_cache,
            )
            if propagated is None:
                return PreparedRootQuery(
                    query=ModelQuery(),
                    model_cache=model_cache,
                    impossible=True,
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
                    query=ModelQuery(),
                    model_cache=model_cache,
                    impossible=True,
                )

            propagated = await self._propagate_filter_to_root_async(
                self._find_path(self.root_model, model),
                target_page.items,
                model_cache,
            )
            if propagated is None:
                return PreparedRootQuery(
                    query=ModelQuery(),
                    model_cache=model_cache,
                    impossible=True,
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
            sortable.source if sortable is not None else None,
            descending,
        )

    def _root_sort_from_source(
        self,
        sort_source: FieldDefinition | None,
        descending: bool,
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
            if root_sort is not None:
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
            f"{step.current_model.__name__}.{step.current_field_name}->"
            f"{step.next_model.__name__}.{step.next_field_name}"
            for step in path
        )
