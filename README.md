# ORMRM

Object-Relational-Mapping-Relational-Mapping (ORMRM) is a tool that can map multiple datasources
to views. It is designed to be used in a microservice architecture, where each microservice has its
own database. ORMRM allows you to create a unified view of your data, without having to worry about
the underlying database structure.

## Usage

Define the datastructures, and the datasources.
The library supports both synchronous and asynchronous data fetching, allowing you to choose the
best approach for your application, either or both can be implemented in the models.

```python
class User(BaseModel):
    datasource = DatasourceMetadata(paginated=True, default_page_size=100)

    id: int = define_field(primary_key=True, filters=[EqualsFilter, InListFilter])
    name: str = define_field(filters=[EqualsFilter, ContainsFilter], sortable=True)
    email: str = define_field(filters=[EqualsFilter])
    posts: List[Post] = create_relation(to=Post, relation_type='one-to-many', foreign_key='author_id')

    @staticmethod
    def list(filters, page=1, page_size=None, sort_by=None) -> DataPage['User']:
        # Implement the logic to fetch users from the datasource based on the filter_context
        pass

    @staticmethod
    async def list_async(filters, page=1, page_size=None, sort_by=None) -> DataPage['User']:
        # Implement the logic to fetch users from the datasource based on the filter_context
        pass

class Post(BaseModel):
    datasource = DatasourceMetadata(paginated=True, default_page_size=100)

    id: int = define_field(primary_key=True, filters=[EqualsFilter, InListFilter])
    title: str = define_field(filters=[EqualsFilter, ContainsFilter], sortable=True)
    published_date: datetime = define_field(filters=[DateTimeRangeFilter], sortable=True)
    content: str = define_field(filters=[EqualsFilter, ContainsFilter])
    author_id: int = define_field(filters=[EqualsFilter, InListFilter])
    author: User = create_relation(to=User, relation_type='many-to-one', foreign_key='author_id')

    @staticmethod
    def list(filters, page=1, page_size=None, sort_by=None) -> DataPage['Post']:
        # Implement the logic to fetch posts from the datasource based on the filter_context
        pass

    @staticmethod
    async def list_async(filters, page=1, page_size=None, sort_by=None) -> DataPage['Post']:
        # Implement the logic to fetch posts from the datasource based on the filter_context
        pass
```

Then, you can create views that combine data from multiple datasources.

```python
posts_with_authors = create_view(
    fields = [
        ViewField(name='post_id', source=Post.id),
        ViewField(name='post_title', source=Post.title),
        ViewField(name='author_name', source=User.name),
        ViewField(name='published_date', source=Post.published_date),
    ],
    filters = [
        ViewFilter(name='author_name', source=User.name, filter_type=ContainsFilter),
        ViewFilter(name='published_date', source=Post.published_date, filter_type=DateTimeRangeFilter),
    ],
    sortables = [
        ViewSortable(name='published_date', source=Post.published_date, default_sort='desc'),
        ViewSortable(name='author_name', source=User.name),
    ],
    pagination = PaginationConfig(default_page_size=20, max_page_size=100),
)
```

And using the view to fetch data.

```python
posts = posts_with_authors.list(
    page=1,
    page_size=10,
    sort_by="-published_date",
)
```

And using these views more complex queries can be made, such as filtering posts by an author name,
while sorting by published date, which use two different datasources.

```python
posts_of_author = await posts_with_authors.list_async(
    page=1,
    page_size=10,
    sort_by="-published_date",
    filters={
        "author_name": "John Doe",
    }
)
```

In this last query, the system first iterates the paginated user datasource to collect all matching
User IDs. It can then query the post datasource with an `author_id` filter and the requested view
page. Both model-level queries and view-level queries return pagination metadata, so the system
knows the total record counts and can stop fetching root records once it has satisfied the requested
view page when the root datasource can serve that page directly.
