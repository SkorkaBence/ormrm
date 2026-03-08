# ORMRM

ORMRM is a Python library for building read-only views across isolated datasources.
It is intended for microservice architectures where related data lives behind separate
APIs and cannot be joined at the database level.

Instead of joining tables, ORMRM plans and executes a datasource pipeline:

- collect records from upstream services when filters or sort order depend on them
- derive root-model filters from those records
- query the root datasource as efficiently as possible
- project the result into a single view shape

## Status

ORMRM is currently pre-alpha.
The API is usable for experimentation, but it should still be treated as unstable.

## Features

- model definitions for API-backed datasources
- one-to-one, one-to-many, and many-to-one relation traversal
- sync and async datasource execution
- paginated model datasources with total item and total page metadata
- paginated views with total item and total page metadata
- cross-datasource filtering
- sortable view fields, including grouped parent-first execution for some multi-sort cases
- inspectable execution plans

## Requirements

- Python `>=3.12`
- no required runtime dependencies at the moment

## Installation

From source:

```bash
pip install .
```

With development and test dependencies:

```bash
pip install ".[test]"
```

## Core Concepts

`BaseModel`
: Declares a datasource-backed model and its fields.

`DatasourceMetadata`
: Describes whether the datasource is paginated and which page sizes it supports.

`View`
: A read-only projection assembled from one root model and any related models.

`DataPage`
: The model-level result type with `items`, `page`, `page_size`, `total_items`, and `total_pages`.

`Page`
: The view-level result type with the same pagination metadata, but projected view rows.

`ViewExecutionPlan`
: The inspectable execution plan that describes how ORMRM will query upstream datasources.

## Quick Start

Define your models and datasource fetchers.
The recommended model contract is to return `DataPage` from `list()` and `list_async()`.

```python
from datetime import datetime

from ormrm import (
    BaseModel,
    ContainsFilter,
    DataPage,
    DatasourceMetadata,
    DateTimeRangeFilter,
    EqualsFilter,
    InListFilter,
    create_relation,
    define_field,
)


class User(BaseModel):
    datasource = DatasourceMetadata(
        paginated=True,
        default_page_size=100,
        max_page_size=500,
    )

    id = define_field(primary_key=True, filters=[EqualsFilter, InListFilter])
    name = define_field(filters=[EqualsFilter, ContainsFilter], sortable=True)
    email = define_field(filters=[EqualsFilter])
    posts = create_relation(
        to="Post",
        relation_type="one-to-many",
        foreign_key="author_id",
    )

    @staticmethod
    def list(filters, *, page=1, page_size=None, sort_by=None) -> DataPage["User"]:
        raise NotImplementedError

    @staticmethod
    async def list_async(
        filters,
        *,
        page=1,
        page_size=None,
        sort_by=None,
    ) -> DataPage["User"]:
        raise NotImplementedError


class Post(BaseModel):
    datasource = DatasourceMetadata(
        paginated=True,
        default_page_size=100,
        max_page_size=500,
    )

    id = define_field(primary_key=True, filters=[EqualsFilter, InListFilter])
    title = define_field(filters=[EqualsFilter, ContainsFilter], sortable=True)
    published_date = define_field(filters=[DateTimeRangeFilter], sortable=True)
    content = define_field(filters=[EqualsFilter, ContainsFilter])
    author_id = define_field(filters=[EqualsFilter, InListFilter])
    author = create_relation(
        to=User,
        relation_type="many-to-one",
        foreign_key="author_id",
    )

    @staticmethod
    def list(filters, *, page=1, page_size=None, sort_by=None) -> DataPage["Post"]:
        raise NotImplementedError

    @staticmethod
    async def list_async(
        filters,
        *,
        page=1,
        page_size=None,
        sort_by=None,
    ) -> DataPage["Post"]:
        raise NotImplementedError
```

Create a view that combines multiple datasources:

```python
from ormrm import (
    PaginationConfig,
    ViewField,
    ViewFilter,
    ViewSortable,
    create_view,
)


posts_with_authors = create_view(
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
        ViewSortable(
            name="published_date",
            source=Post.published_date,
            default_sort="desc",
        ),
        ViewSortable(name="author_name", source=User.name),
        ViewSortable(name="post_title", source=Post.title),
    ],
    pagination=PaginationConfig(default_page_size=20, max_page_size=100),
)
```

Run a synchronous view query:

```python
page = posts_with_authors.list(
    page=1,
    page_size=10,
    sort_by="-published_date",
)

print(page.items)
print(page.total_items, page.total_pages)
```

Run an asynchronous view query:

```python
page = await posts_with_authors.list_async(
    page=1,
    page_size=10,
    sort_by="-published_date",
    filters={"author_name": "John Doe"},
)
```

## Pagination Model

ORMRM assumes upstream APIs are often paginated.
Because of that, both model queries and view queries expose pagination metadata.

Model datasources:

- declare pagination support with `DatasourceMetadata`
- receive `page`, `page_size`, and `sort_by`
- should return `DataPage` whenever possible

View queries:

- return `Page`
- include `items`, `page`, `page_size`, `total_items`, and `total_pages`
- stop early when the chosen execution plan can satisfy the requested page without collecting all root records

When ORMRM must fully collect a related datasource to derive root filters, it prefers that datasource's
largest supported page size to reduce the number of upstream API calls.

## Execution Planning And Inspect

Every view query is first turned into a `ViewExecutionPlan`.
That plan is independent from sync or async execution and can be inspected directly.

```python
plan = posts_with_authors.inspect(
    page=1,
    page_size=10,
    sort_by=["author_name", "post_title"],
    filters={"author_name": "john"},
)

print(plan)
for step in plan.steps:
    print(step)
```

The inspect output is meant for manual debugging and understanding why ORMRM chose a specific
query pipeline.

## Current Optimization Behavior

ORMRM currently supports three main execution modes:

- `root_page`: the root datasource can serve the requested page directly
- `grouped_root_page`: the first sort key is on a related parent model and the remaining sort keys are on the root model
- `collect_root`: the full root record set must be collected and sorted in memory

In related-filter scenarios, ORMRM collects matching related records first, derives a root filter,
and then queries the root datasource with that filter.

## Limitations

- the project is pre-alpha and the public API may change
- sorting by multi-valued related fields is not supported
- grouped parent-first optimization currently only handles a limited class of multi-sort plans
- execution is read-only; ORMRM does not write back to upstream services

## Development

Create or activate a virtual environment, then install development dependencies:

```bash
pip install ".[test]"
```

Run the full test and typing check suite:

```bash
python3 -m pytest --mypy --timeout 300
```

Format the package code:

```bash
python3 -m black ormrm
```

## Type Checking

The codebase is typed and checked with `mypy`.
The intended usage pattern is that model fetchers return typed `DataPage[TModel]` results and view
queries return typed pagination metadata plus projected dictionaries.

## Public API

The package currently exports the main building blocks from `ormrm` directly:

- `BaseModel`
- `DataPage`, `DatasourceMetadata`
- `define_field`, `create_relation`
- `View`, `create_view`
- `ViewField`, `ViewFilter`, `ViewSortable`, `PaginationConfig`, `Page`
- `ModelQuery`, `BoundFilter`
- `ViewExecutionPlan` and the individual plan step classes

## License

MIT. See [LICENSE](LICENSE).
