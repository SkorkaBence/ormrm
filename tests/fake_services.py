from __future__ import annotations

import math
from datetime import datetime

from ormrm import (
    BaseModel,
    ContainsFilter,
    DataPage,
    DateTimeRange,
    DateTimeRangeFilter,
    DatasourceMetadata,
    EqualsFilter,
    InListFilter,
    PaginationConfig,
    ViewField,
    ViewFilter,
    ViewSortable,
    create_relation,
    create_view,
    define_field,
)
from ormrm.query import BoundFilter, ModelQuery


USERS = [
    {"id": 1, "name": "Alice Adams", "email": "alice@example.com"},
    {"id": 2, "name": "Bob Brown", "email": "bob@example.com"},
    {"id": 3, "name": "Carla Cruz", "email": "carla@example.com"},
]

POSTS = [
    {
        "id": 101,
        "title": "Async Patterns",
        "published_date": datetime(2024, 1, 10, 12, 0, 0),
        "content": "Using async in services",
        "author_id": 1,
    },
    {
        "id": 102,
        "title": "Distributed Tracing",
        "published_date": datetime(2024, 1, 20, 8, 30, 0),
        "content": "Tracing requests across services",
        "author_id": 2,
    },
    {
        "id": 103,
        "title": "Testing Strategies",
        "published_date": datetime(2024, 2, 3, 9, 15, 0),
        "content": "Reliable tests for APIs",
        "author_id": 1,
    },
    {
        "id": 104,
        "title": "Caching Basics",
        "published_date": datetime(2024, 2, 15, 17, 45, 0),
        "content": "Cache invalidation tradeoffs",
        "author_id": 3,
    },
]

REQUEST_LOG: list[str] = []


def _matches_clause(candidate: object, bound_filter: BoundFilter) -> bool:
    if bound_filter.filter_type is EqualsFilter:
        return candidate == bound_filter.value
    if bound_filter.filter_type is ContainsFilter:
        if candidate is None:
            return False
        return str(bound_filter.value).lower() in str(candidate).lower()
    if bound_filter.filter_type is InListFilter:
        return candidate in bound_filter.value
    if bound_filter.filter_type is DateTimeRangeFilter:
        date_range = bound_filter.value
        if candidate is None:
            return False
        if date_range.start is not None and candidate < date_range.start:
            return False
        if date_range.end is not None and candidate > date_range.end:
            return False
        return True
    raise AssertionError(f"Unsupported filter type in tests: {bound_filter.filter_type!r}")


def _matches_query(row: dict[str, object], query: ModelQuery) -> bool:
    for field_name, filters in query.items():
        candidate = row[field_name]
        if not all(_matches_clause(candidate, bound_filter) for bound_filter in filters):
            return False
    return True


def _format_filter_value(value: object) -> str:
    if isinstance(value, str):
        return f"'{value}'"
    if isinstance(value, DateTimeRange):
        return f"DateTimeRange(start={value.start!r}, end={value.end!r})"
    if isinstance(value, set):
        ordered = sorted(value)
        return f"{{{', '.join(repr(item) for item in ordered)}}}"
    return repr(value)


def _format_sort(sort_by: str | tuple[str, ...] | list[str] | None) -> str:
    if sort_by is None:
        return "[]"
    if isinstance(sort_by, str):
        return repr([sort_by])
    return repr(list(sort_by))


def _format_query_log(
    model_name: str,
    query: ModelQuery,
    *,
    page: int,
    page_size: int | None,
    sort_by: str | tuple[str, ...] | list[str] | None,
) -> str:
    parts: list[str] = []
    for field_name, filters in query.items():
        for bound_filter in filters:
            parts.append(f"{field_name}={_format_filter_value(bound_filter.value)}")
    return (
        f"{model_name}{{page={page},page_size={page_size},sort={_format_sort(sort_by)},"
        f"filters=[{','.join(parts)}]}}"
    )


def _build_page(
    model: type[BaseModel],
    rows: list[dict[str, object]],
    *,
    page: int,
    page_size: int | None,
    sort_by: str | tuple[str, ...] | list[str] | None,
) -> DataPage[BaseModel]:
    sort_values: tuple[str, ...]
    if sort_by is None:
        sort_values = ()
    elif isinstance(sort_by, str):
        sort_values = (sort_by,)
    else:
        sort_values = tuple(sort_by)
    for current_sort in reversed(sort_values):
        reverse = current_sort.startswith("-")
        field_name = current_sort[1:] if reverse else current_sort
        rows = sorted(rows, key=lambda row: row[field_name], reverse=reverse)

    resolved_page_size = page_size or model.datasource.default_page_size or len(rows) or 1
    total_items = len(rows)
    total_pages = math.ceil(total_items / resolved_page_size) if total_items else 0
    start = (page - 1) * resolved_page_size
    end = start + resolved_page_size
    page_rows = rows[start:end]
    return DataPage(
        items=[model(**row) for row in page_rows],
        page=page,
        page_size=resolved_page_size,
        total_items=total_items,
        total_pages=total_pages,
    )


class User(BaseModel):
    datasource = DatasourceMetadata(paginated=True, default_page_size=1, max_page_size=2)
    sync_calls = 0
    async_calls = 0
    requested_pages: list[int] = []

    id: int = define_field(primary_key=True, filters=[EqualsFilter, InListFilter])
    name: str = define_field(filters=[EqualsFilter, ContainsFilter], sortable=True)
    email: str = define_field(filters=[EqualsFilter])
    posts = create_relation(to="Post", relation_type="one-to-many", foreign_key="author_id")

    @staticmethod
    def list(
        filters: ModelQuery,
        *,
        page: int = 1,
        page_size: int | None = None,
        sort_by: str | tuple[str, ...] | list[str] | None = None,
    ) -> DataPage["User"]:
        User.sync_calls += 1
        User.requested_pages.append(page)
        REQUEST_LOG.append(_format_query_log("user", filters, page=page, page_size=page_size, sort_by=sort_by))
        rows = [row for row in USERS if _matches_query(row, filters)]
        return _build_page(User, rows, page=page, page_size=page_size, sort_by=sort_by)

    @staticmethod
    async def list_async(
        filters: ModelQuery,
        *,
        page: int = 1,
        page_size: int | None = None,
        sort_by: str | tuple[str, ...] | list[str] | None = None,
    ) -> DataPage["User"]:
        User.async_calls += 1
        User.requested_pages.append(page)
        REQUEST_LOG.append(_format_query_log("user_async", filters, page=page, page_size=page_size, sort_by=sort_by))
        rows = [row for row in USERS if _matches_query(row, filters)]
        return _build_page(User, rows, page=page, page_size=page_size, sort_by=sort_by)


class Post(BaseModel):
    datasource = DatasourceMetadata(paginated=True, default_page_size=1, max_page_size=3)
    sync_calls = 0
    async_calls = 0
    requested_pages: list[int] = []

    id: int = define_field(primary_key=True, filters=[EqualsFilter, InListFilter])
    title: str = define_field(filters=[EqualsFilter, ContainsFilter], sortable=True)
    published_date: datetime = define_field(filters=[DateTimeRangeFilter], sortable=True)
    content: str = define_field(filters=[EqualsFilter, ContainsFilter])
    author_id: int = define_field(filters=[EqualsFilter, InListFilter])
    author = create_relation(to=User, relation_type="many-to-one", foreign_key="author_id")

    @staticmethod
    def list(
        filters: ModelQuery,
        *,
        page: int = 1,
        page_size: int | None = None,
        sort_by: str | tuple[str, ...] | list[str] | None = None,
    ) -> DataPage["Post"]:
        Post.sync_calls += 1
        Post.requested_pages.append(page)
        REQUEST_LOG.append(_format_query_log("post", filters, page=page, page_size=page_size, sort_by=sort_by))
        rows = [row for row in POSTS if _matches_query(row, filters)]
        return _build_page(Post, rows, page=page, page_size=page_size, sort_by=sort_by)

    @staticmethod
    async def list_async(
        filters: ModelQuery,
        *,
        page: int = 1,
        page_size: int | None = None,
        sort_by: str | tuple[str, ...] | list[str] | None = None,
    ) -> DataPage["Post"]:
        Post.async_calls += 1
        Post.requested_pages.append(page)
        REQUEST_LOG.append(_format_query_log("post_async", filters, page=page, page_size=page_size, sort_by=sort_by))
        rows = [row for row in POSTS if _matches_query(row, filters)]
        return _build_page(Post, rows, page=page, page_size=page_size, sort_by=sort_by)


def reset_call_counters() -> None:
    REQUEST_LOG.clear()
    User.sync_calls = 0
    User.async_calls = 0
    User.requested_pages = []
    Post.sync_calls = 0
    Post.async_calls = 0
    Post.requested_pages = []


def build_posts_with_authors_view():
    return create_view(
        fields=[
            ViewField(name="post_id", source=Post.id),
            ViewField(name="post_title", source=Post.title),
            ViewField(name="author_name", source=User.name),
            ViewField(name="published_date", source=Post.published_date),
        ],
        filters=[
            ViewFilter(name="author_name", source=User.name, filter_type=ContainsFilter),
            ViewFilter(
                name="published_date",
                source=Post.published_date,
                filter_type=DateTimeRangeFilter,
            ),
        ],
        sortables=[
            ViewSortable(name="published_date", source=Post.published_date, default_sort="desc"),
            ViewSortable(name="author_name", source=User.name),
            ViewSortable(name="post_title", source=Post.title),
        ],
        pagination=PaginationConfig(default_page_size=2, max_page_size=5),
    )


def build_authors_with_posts_view():
    return create_view(
        fields=[
            ViewField(name="author_name", source=User.name),
            ViewField(name="email", source=User.email),
        ],
        filters=[
            ViewFilter(name="post_title", source=Post.title, filter_type=ContainsFilter),
        ],
        sortables=[
            ViewSortable(name="author_name", source=User.name),
        ],
        pagination=PaginationConfig(default_page_size=10, max_page_size=20),
    )


def january_range() -> DateTimeRange:
    return DateTimeRange(
        start=datetime(2024, 1, 1, 0, 0, 0),
        end=datetime(2024, 1, 31, 23, 59, 59),
    )
