from __future__ import annotations

import builtins
from collections.abc import Mapping, Sequence
from typing import Any, ClassVar, cast

from .datasource import DataPage, DatasourceMetadata
from .query import ModelQuery
from .schema import FieldDefinition, RelationDefinition


class ModelMeta(type):
    def __new__(
        mcls,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
    ) -> type:
        cls = cast(type["BaseModel"], super().__new__(mcls, name, bases, namespace))

        fields: dict[str, FieldDefinition] = {}
        relations: dict[str, RelationDefinition] = {}

        for base in bases:
            fields.update(getattr(base, "__fields__", {}))
            relations.update(getattr(base, "__relations__", {}))

        for attribute_name, attribute_value in namespace.items():
            if isinstance(attribute_value, FieldDefinition):
                attribute_value.bind(cls, attribute_name)
                fields[attribute_name] = attribute_value
            if isinstance(attribute_value, RelationDefinition):
                attribute_value.bind(cls, attribute_name)
                relations[attribute_name] = attribute_value

        cls.__fields__ = fields
        cls.__relations__ = relations

        if name != "BaseModel":
            existing = BaseModel._registry.get(name)
            if existing is not None and existing is not cls:
                raise ValueError(
                    f"Model name '{name}' is already registered by "
                    f"{existing.__module__}.{existing.__qualname__}"
                )
            BaseModel._registry[name] = cls

        return cls


class BaseModel(metaclass=ModelMeta):
    __fields__: ClassVar[dict[str, FieldDefinition]]
    __relations__: ClassVar[dict[str, RelationDefinition]]
    _registry: ClassVar[dict[str, type["BaseModel"]]] = {}
    datasource: ClassVar[DatasourceMetadata] = DatasourceMetadata()

    def __init__(self, **values: Any) -> None:
        for field_name in self.__fields__:
            setattr(self, field_name, values.get(field_name))

    @classmethod
    def get_registered_model(cls, name: str) -> type["BaseModel"]:
        try:
            return cls._registry[name]
        except KeyError as exc:
            raise ValueError(f"Unknown model '{name}'") from exc

    @classmethod
    def primary_key_field(cls) -> FieldDefinition:
        for field_definition in cls.__fields__.values():
            if field_definition.primary_key:
                return field_definition
        raise ValueError(f"Model {cls.__name__} does not define a primary key")

    @classmethod
    def field(cls, name: str) -> FieldDefinition:
        try:
            return cls.__fields__[name]
        except KeyError as exc:
            raise ValueError(f"Model {cls.__name__} has no field '{name}'") from exc

    @classmethod
    def relation(cls, name: str) -> RelationDefinition:
        try:
            return cls.__relations__[name]
        except KeyError as exc:
            raise ValueError(f"Model {cls.__name__} has no relation '{name}'") from exc

    @classmethod
    def ensure_instance(cls, record: Any) -> "BaseModel":
        if isinstance(record, cls):
            return record
        if isinstance(record, Mapping):
            return cls(**record)
        raise TypeError(
            f"{cls.__name__}.list() returned an unsupported record type: {type(record)!r}"
        )

    @classmethod
    def normalize_records(cls, records: Sequence[Any]) -> builtins.list["BaseModel"]:
        pk_name = cls.primary_key_field().bound_name
        normalized = [cls.ensure_instance(record) for record in records]
        deduplicated: dict[Any, BaseModel] = {}
        for record in normalized:
            deduplicated[getattr(record, pk_name)] = record
        return list(deduplicated.values())

    @staticmethod
    def list(
        filters: ModelQuery,
        *,
        page: int = 1,
        page_size: int | None = None,
        sort_by: str | tuple[str, ...] | builtins.list[str] | None = None,
    ) -> DataPage[Any] | Sequence[Any]:
        raise NotImplementedError

    @staticmethod
    async def list_async(
        filters: ModelQuery,
        *,
        page: int = 1,
        page_size: int | None = None,
        sort_by: str | tuple[str, ...] | builtins.list[str] | None = None,
    ) -> DataPage[Any] | Sequence[Any]:
        raise NotImplementedError

    @classmethod
    def normalize_page(
        cls,
        page_result: DataPage[Any] | Sequence[Any],
    ) -> DataPage["BaseModel"]:
        if isinstance(page_result, DataPage):
            records = cls.normalize_records(page_result.items)
            resolved_page_size = page_result.page_size or len(records) or 1
            return DataPage(
                items=records,
                page=page_result.page,
                page_size=resolved_page_size,
                total_items=page_result.total_items,
                total_pages=page_result.total_pages,
            )

        records = cls.normalize_records(page_result)
        total_items = len(records)
        return DataPage(
            items=records,
            page=1,
            page_size=total_items or 1,
            total_items=total_items,
            total_pages=1 if total_items else 0,
        )
