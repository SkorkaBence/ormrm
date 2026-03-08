from datetime import datetime

from .fake_services import (
    REQUEST_LOG,
    Post,
    User,
    build_authors_with_posts_view,
    build_posts_with_authors_view,
    january_range,
    reset_call_counters,
)


def test_sync_view_filters_across_related_models_and_uses_root_page() -> None:
    reset_call_counters()
    view = build_posts_with_authors_view()

    page = view.list(
        filters={"author_name": "a"},
        sort_by="-published_date",
        page=2,
        page_size=1,
    )

    assert page.total_items == 3
    assert page.total_pages == 3
    assert page.items == [
        {
            "post_id": 103,
            "post_title": "Testing Strategies",
            "author_name": "Alice Adams",
            "published_date": datetime(2024, 2, 3, 9, 15, 0),
        }
    ]
    assert User.requested_pages == [1]
    assert Post.requested_pages == [2]


def test_sync_view_applies_root_filters_and_default_sort() -> None:
    reset_call_counters()
    view = build_posts_with_authors_view()

    page = view.list(filters={"published_date": january_range()})

    assert page.total_items == 2
    assert page.total_pages == 1
    assert [item["post_id"] for item in page.items] == [102, 101]
    assert Post.requested_pages == [1]


def test_sync_view_supports_reverse_relation_filtering() -> None:
    reset_call_counters()
    view = build_authors_with_posts_view()

    page = view.list(filters={"post_title": "tracing"})

    assert page.total_items == 1
    assert page.items == [
        {
            "author_name": "Bob Brown",
            "email": "bob@example.com",
        }
    ]
    assert Post.requested_pages == [1]


def test_sync_view_resolves_related_filters_before_root_filters() -> None:
    reset_call_counters()
    view = build_posts_with_authors_view()

    page = view.list(
        filters={
            "author_name": "a",
            "published_date": january_range(),
        },
        sort_by="-published_date",
        page=1,
        page_size=1,
    )

    assert page.total_items == 1
    assert page.total_pages == 1
    assert page.items == [
        {
            "post_id": 101,
            "post_title": "Async Patterns",
            "author_name": "Alice Adams",
            "published_date": datetime(2024, 1, 10, 12, 0, 0),
        }
    ]
    assert REQUEST_LOG == [
        "user{page=1,page_size=2,sort=[],filters=[name='a']}",
        "post{page=1,page_size=1,sort=['-published_date'],filters=[published_date=DateTimeRange(start=datetime.datetime(2024, 1, 1, 0, 0), end=datetime.datetime(2024, 1, 31, 23, 59, 59)),author_id={1, 3}]}",
    ]


def test_view_inspect_returns_readable_execution_plan() -> None:
    view = build_posts_with_authors_view()

    plan = view.inspect(
        filters={
            "author_name": "a",
            "published_date": january_range(),
        },
        sort_by="-published_date",
        page=1,
        page_size=1,
    )

    assert repr(plan) == "\n".join(
        [
            "ViewExecutionPlan(root=Post, page=1, page_size=1, sort_by='-published_date')",
            "  1. CollectRecordsStep(model=User, mode='all', page_size=2, filters=[name ContainsFilter 'a'], purpose='collect matching related records')",
            "  2. DeriveRootFilterStep(source=User, target=Post.author_id, path='Post.author_id->User.id')",
            "  3. FetchRootRecordsStep(model=Post, mode='page', page=1, page_size=1, filters=[published_date DateTimeRangeFilter DateTimeRange(start=datetime.datetime(2024, 1, 1, 0, 0), end=datetime.datetime(2024, 1, 31, 23, 59, 59))], sort_by='-published_date', derived_filters=['author_id from User'])",
            "  4. ProjectViewStep(fields=['post_id <- Post.id', 'post_title <- Post.title', 'author_name <- User.name', 'published_date <- Post.published_date'])",
        ]
    )


def test_sync_view_groups_root_fetch_by_related_sort_and_stops_after_page() -> None:
    reset_call_counters()
    view = build_posts_with_authors_view()

    page = view.list(
        sort_by=["author_name", "post_title"],
        page=1,
        page_size=3,
    )

    assert page.total_items == 4
    assert page.total_pages == 2
    assert page.items == [
        {
            "post_id": 101,
            "post_title": "Async Patterns",
            "author_name": "Alice Adams",
            "published_date": datetime(2024, 1, 10, 12, 0, 0),
        },
        {
            "post_id": 103,
            "post_title": "Testing Strategies",
            "author_name": "Alice Adams",
            "published_date": datetime(2024, 2, 3, 9, 15, 0),
        },
        {
            "post_id": 102,
            "post_title": "Distributed Tracing",
            "author_name": "Bob Brown",
            "published_date": datetime(2024, 1, 20, 8, 30, 0),
        },
    ]
    assert REQUEST_LOG == [
        "user{page=1,page_size=2,sort=['name'],filters=[]}",
        "post{page=1,page_size=3,sort=['title'],filters=[author_id=1]}",
        "post{page=1,page_size=1,sort=['title'],filters=[author_id=2]}",
        "user{page=2,page_size=2,sort=['name'],filters=[]}",
        "post{page=1,page_size=1,sort=['title'],filters=[author_id=3]}",
    ]
