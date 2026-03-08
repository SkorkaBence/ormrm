from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ormrm.plans.plan import ViewExecutionPlan, ViewRequest
from ormrm.plans.steps import (
    CollectRecordsStep,
    DeriveRootFilterStep,
    FetchRootRecordsStep,
    ProjectViewStep,
    SortRecordsStep,
)
from ormrm.query import ModelQuery

if TYPE_CHECKING:
    from ormrm.views import View


class ViewPlanner:
    def __init__(self, view: "View") -> None:
        self.view = view

    def build(
        self,
        *,
        page: int = 1,
        page_size: int | None = None,
        sort_by: str | list[str] | tuple[str, ...] | None = None,
        filters: dict[str, Any] | None = None,
    ) -> ViewExecutionPlan:
        request_filters = filters or {}
        normalized_page, normalized_page_size = self.view._normalize_pagination(page, page_size)
        grouped_filters = self.view._group_requested_filters(request_filters)
        root_filters = tuple(grouped_filters.get(self.view.root_model, ()))
        resolved_sorts = self.view._resolve_sorts(sort_by)
        sort_sources = tuple(sortable.source for sortable, _ in resolved_sorts)
        descending_flags = tuple(descending for _, descending in resolved_sorts)
        can_page_root = self.view._can_page_root_sources(sort_sources)
        execution_mode = "root_page"
        if can_page_root:
            root_sort_by = self.view._serialize_root_sorts(resolved_sorts)
        elif self.view._can_grouped_root_page(resolved_sorts):
            execution_mode = "grouped_root_page"
            root_sort_by = self.view._serialize_root_sorts(resolved_sorts[1:])
        else:
            execution_mode = "collect_root"
            root_sort_by = ()

        steps = []
        derived_filters: list[str] = []
        grouped_sort_model = None
        if execution_mode == "grouped_root_page":
            grouped_sort_model = self.view._model_for_field(sort_sources[0])

        if grouped_sort_model is not None and grouped_sort_model not in grouped_filters:
            steps.append(
                CollectRecordsStep(
                    model_name=grouped_sort_model.__name__,
                    query=ModelQuery(),
                    mode="all",
                    page_size=grouped_sort_model.datasource.max_page_size or grouped_sort_model.datasource.default_page_size,
                    sort_by=self.view._serialize_root_sorts_for_step([resolved_sorts[0]]),
                    purpose="walk sorted parent groups",
                )
            )

        for model, bound_filters in grouped_filters.items():
            if model is self.view.root_model:
                continue
            query = ModelQuery.from_bound_filters(bound_filters)
            steps.append(
                CollectRecordsStep(
                    model_name=model.__name__,
                    query=query,
                    mode="all",
                    page_size=model.datasource.max_page_size or model.datasource.default_page_size,
                    sort_by=self.view._serialize_root_sorts_for_step(
                        [
                            sort_expression
                            for sort_expression in resolved_sorts
                            if sort_expression[0].source.owner is model
                        ]
                    ),
                    purpose="collect matching related records",
                )
            )
            path = self.view._find_path(self.view.root_model, model)
            root_field_name = self.view._root_field_for_path(path)
            steps.append(
                DeriveRootFilterStep(
                    source_model_name=model.__name__,
                    root_model_name=self.view.root_model.__name__,
                    root_field_name=root_field_name,
                    path=self.view._path_descriptions(path),
                )
            )
            derived_filters.append(f"{root_field_name} from {model.__name__}")

        steps.append(
            FetchRootRecordsStep(
                model_name=self.view.root_model.__name__,
                mode="page" if execution_mode == "root_page" else ("grouped-page" if execution_mode == "grouped_root_page" else "all"),
                query=ModelQuery.from_bound_filters(root_filters),
                page=normalized_page,
                page_size=normalized_page_size,
                sort_by=self.view._serialize_root_sorts_for_step(resolved_sorts if execution_mode == "root_page" else resolved_sorts[1:]),
                derived_filters=tuple(derived_filters),
            )
        )

        if execution_mode == "collect_root" and resolved_sorts:
            steps.append(
                SortRecordsStep(
                    source=", ".join(sortable.source.qualified_name for sortable, _ in resolved_sorts),
                    direction=", ".join("desc" if descending else "asc" for _, descending in resolved_sorts),
                )
            )

        steps.append(
            ProjectViewStep(
                fields=tuple(f"{field.name} <- {field.source.qualified_name}" for field in self.view.fields)
            )
        )

        return ViewExecutionPlan(
            root_model=self.view.root_model,
            request=ViewRequest(
                page=normalized_page,
                page_size=normalized_page_size,
                sort_by=sort_by,
                filters=request_filters,
            ),
            grouped_filters=tuple((model, tuple(filters)) for model, filters in grouped_filters.items()),
            root_filters=root_filters,
            sort_sources=sort_sources,
            descending_flags=descending_flags,
            execution_mode=execution_mode,
            can_page_root=can_page_root,
            root_sort_by=root_sort_by,
            steps=tuple(steps),
        )
