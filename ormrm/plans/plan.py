from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..models import BaseModel
from ..query import BoundFilter, ModelQuery
from ..schema import FieldDefinition
from .steps import PlanStep


@dataclass(frozen=True, repr=False)
class ViewRequest:
    """Normalized view query input.

    Attributes:
        page:
            The requested output page on the final view result.
        page_size:
            The requested output page size on the final view result.
        sort_by:
            The original user-facing sort expression or expressions, such as
            ``"-published_date"`` or ``["author_name", "title"]``.
        filters:
            The raw view filter values keyed by view filter name.
    """

    page: int
    page_size: int
    sort_by: str | list[str] | tuple[str, ...] | None
    filters: dict[str, Any]

    def __repr__(self) -> str:
        return (
            "ViewRequest("
            f"page={self.page}, "
            f"page_size={self.page_size}, "
            f"sort_by={self.sort_by!r}, "
            f"filters={self.filters!r}"
            ")"
        )


@dataclass(frozen=True, repr=False)
class ViewExecutionPlan:
    """Sync/async-independent execution strategy for a view query.

    Attributes:
        root_model:
            The model that anchors the view execution. The root datasource is the
            last datasource queried before projection into the final view rows.
        request:
            The normalized view request after pagination defaults and validation.
        grouped_filters:
            View filters grouped by their source model. Execution uses this to decide
            which upstream datasources must be queried before the root datasource.
        root_filters:
            The subset of filters that are already native to the root model and can
            be sent directly to the root datasource without first consulting another
            datasource.
        sort_sources:
            The ordered model fields that define the effective sort for this
            request, after applying any default sort configuration.
        descending_flags:
            Sort directions aligned with ``sort_sources``. ``True`` means the
            corresponding sort field is descending.
        execution_mode:
            The strategy chosen for this request.

            ``"root_page"``:
                The root datasource can serve the requested page directly.
            ``"grouped_root_page"``:
                The first sort key comes from a related parent model and the
                remaining sort keys come from the root model, so execution walks
                parent groups in sorted order and fetches root rows incrementally.
            ``"collect_root"``:
                The executor must collect the full root record set before sorting
                and slicing the requested page.
        can_page_root:
            Whether the executor can ask the root datasource directly for the
            requested view page instead of collecting all root records first.

            This is only true when the root datasource is paginated and the final
            ordering can be satisfied by the root datasource itself. If the final
            ordering depends on a related model, the executor must collect the full
            root record set and sort in memory before slicing the requested view
            page.
        root_sort_by:
            The sort expression or expressions that can be passed through to the
            root datasource. In grouped execution this contains only the root-side
            secondary sort keys that are valid within each parent group.
        steps:
            Human-readable plan steps describing the data pipeline. These steps are
            meant for inspection and explainability, while execution uses the other
            structured fields in this plan to run the query.
    """

    root_model: type[BaseModel]
    request: ViewRequest
    grouped_filters: tuple[tuple[type[BaseModel], tuple[BoundFilter, ...]], ...]
    root_filters: tuple[BoundFilter, ...]
    sort_sources: tuple[FieldDefinition, ...]
    descending_flags: tuple[bool, ...]
    execution_mode: str
    can_page_root: bool
    root_sort_by: tuple[str, ...]
    steps: tuple[PlanStep, ...]

    def grouped_filter_map(self) -> dict[type[BaseModel], list[BoundFilter]]:
        return {model: list(filters) for model, filters in self.grouped_filters}

    def root_query(self) -> ModelQuery:
        return ModelQuery.from_bound_filters(self.root_filters)

    def __repr__(self) -> str:
        lines = [
            "ViewExecutionPlan("
            f"root={self.root_model.__name__}, "
            f"page={self.request.page}, "
            f"page_size={self.request.page_size}, "
            f"sort_by={self.request.sort_by!r}"
            ")"
        ]
        for index, step in enumerate(self.steps, start=1):
            lines.append(f"  {index}. {step!r}")
        return "\n".join(lines)
