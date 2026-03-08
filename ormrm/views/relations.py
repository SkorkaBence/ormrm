from __future__ import annotations

import builtins
import inspect
from collections import deque
from collections.abc import Sequence
from typing import Any, Awaitable, cast

from ..datasource import DataPage
from ..filters import InListFilter
from ..models import BaseModel
from ..query import BoundFilter, ModelQuery
from ..schema import FieldDefinition, RelationDefinition

from .support import ViewSupport
from .types import PathCache, PathIndex, RelationStep, ViewSortable


class ViewRelationMixin(ViewSupport):
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
        current_records: builtins.list[BaseModel] = [root_record]
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
        current_records: builtins.list[BaseModel] = [root_record]
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
            return self._rebuilt_collection_page(model, items, first_page)

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
        return self._rebuilt_collection_page(model, items, first_page)

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
            return self._rebuilt_collection_page(model, items, first_page)

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
        return self._rebuilt_collection_page(model, items, first_page)

    def _rebuilt_collection_page(
        self,
        model: type[BaseModel],
        items: builtins.list[BaseModel],
        seed_page: DataPage[BaseModel],
    ) -> DataPage[BaseModel]:
        return DataPage(
            items=model.normalize_records(items),
            page=1,
            page_size=seed_page.page_size,
            total_items=seed_page.total_items,
            total_pages=seed_page.total_pages,
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
                    model,
                    query,
                    page=page,
                    page_size=page_size,
                    sort_by=sort_by,
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
