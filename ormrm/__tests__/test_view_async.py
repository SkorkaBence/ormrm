import asyncio
from datetime import datetime

from .fake_services import Post, User, build_posts_with_authors_view, reset_call_counters


def test_async_view_uses_async_datasource_methods() -> None:
    reset_call_counters()
    view = build_posts_with_authors_view()

    page = asyncio.run(
        view.list_async(
            filters={"author_name": "a"},
            sort_by="-published_date",
            page=1,
            page_size=1,
        )
    )

    assert page.total_items == 3
    assert page.total_pages == 3
    assert page.items == [
        {
            "post_id": 104,
            "post_title": "Caching Basics",
            "author_name": "Carla Cruz",
            "published_date": datetime(2024, 2, 15, 17, 45, 0),
        }
    ]
    assert User.async_calls > 0
    assert Post.async_calls > 0
    assert User.sync_calls == 0
    assert Post.sync_calls == 0
    assert User.requested_pages == [1]
    assert Post.requested_pages == [1]
