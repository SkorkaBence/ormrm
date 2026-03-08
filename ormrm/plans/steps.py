from __future__ import annotations

from dataclasses import dataclass

from ..query import ModelQuery


class PlanStep:
    """Base class for inspectable execution plan steps."""

    def __repr__(self) -> str:
        return self.render()

    def render(self) -> str:
        raise NotImplementedError


@dataclass(frozen=True, repr=False)
class CollectRecordsStep(PlanStep):
    """Collect records from an upstream datasource.

    ``mode='all'`` means the datasource will be iterated until all matching
    records have been collected. ``mode='page'`` means only a single page is
    required for that step.
    """

    model_name: str
    query: ModelQuery
    mode: str
    page_size: int | None
    page: int | None = None
    sort_by: str | None = None
    purpose: str | None = None

    def render(self) -> str:
        parts = [f"model={self.model_name}", f"mode='{self.mode}'"]
        if self.page is not None:
            parts.append(f"page={self.page}")
        if self.page_size is not None:
            parts.append(f"page_size={self.page_size}")
        if self.sort_by is not None:
            parts.append(f"sort_by={self.sort_by!r}")
        parts.append(f"filters={self.query!r}")
        if self.purpose is not None:
            parts.append(f"purpose={self.purpose!r}")
        return f"CollectRecordsStep({', '.join(parts)})"


@dataclass(frozen=True, repr=False)
class DeriveRootFilterStep(PlanStep):
    """Describe how related-model results are converted into a root-model filter."""

    source_model_name: str
    root_model_name: str
    root_field_name: str
    path: tuple[str, ...]

    def render(self) -> str:
        path_text = " -> ".join(self.path)
        return (
            "DeriveRootFilterStep("
            f"source={self.source_model_name}, "
            f"target={self.root_model_name}.{self.root_field_name}, "
            f"path='{path_text}'"
            ")"
        )


@dataclass(frozen=True, repr=False)
class FetchRootRecordsStep(PlanStep):
    """Fetch from the root datasource using native root filters plus derived filters."""

    model_name: str
    mode: str
    query: ModelQuery
    page: int
    page_size: int
    sort_by: str | None
    derived_filters: tuple[str, ...] = ()

    def render(self) -> str:
        parts = [
            f"model={self.model_name}",
            f"mode='{self.mode}'",
            f"page={self.page}",
            f"page_size={self.page_size}",
            f"filters={self.query!r}",
        ]
        if self.sort_by is not None:
            parts.append(f"sort_by={self.sort_by!r}")
        if self.derived_filters:
            parts.append(f"derived_filters={list(self.derived_filters)!r}")
        return f"FetchRootRecordsStep({', '.join(parts)})"


@dataclass(frozen=True, repr=False)
class SortRecordsStep(PlanStep):
    """Sort collected root records in memory when upstream paging cannot satisfy it."""

    source: str
    direction: str

    def render(self) -> str:
        return f"SortRecordsStep(source={self.source!r}, direction='{self.direction}')"


@dataclass(frozen=True, repr=False)
class ProjectViewStep(PlanStep):
    """Project root and related model fields into the final view row shape."""

    fields: tuple[str, ...]

    def render(self) -> str:
        return f"ProjectViewStep(fields={list(self.fields)!r})"
