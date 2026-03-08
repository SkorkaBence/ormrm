from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable

from .filters import BaseFilter

if TYPE_CHECKING:
    from ormrm.models import BaseModel


RelationTarget = type["BaseModel"] | str | Callable[[], type["BaseModel"]]


@dataclass(eq=False)
class FieldDefinition:
    primary_key: bool = False
    filters: tuple[type[BaseFilter], ...] = ()
    sortable: bool = False
    owner: type["BaseModel"] | None = field(default=None, init=False, repr=False)
    name: str | None = field(default=None, init=False)

    def bind(self, owner: type["BaseModel"], name: str) -> None:
        if self.owner is not None and self.owner is not owner:
            raise ValueError(
                f"FieldDefinition '{self.name}' is already bound to "
                f"{self.owner.__name__}, cannot rebind to {owner.__name__}"
            )
        self.owner = owner
        self.name = name

    @property
    def qualified_name(self) -> str:
        if self.owner is None or self.name is None:
            raise ValueError("FieldDefinition is not bound to a model")
        return f"{self.owner.__name__}.{self.name}"

    @property
    def bound_name(self) -> str:
        if self.name is None:
            raise ValueError("FieldDefinition is not bound to a model")
        return self.name


@dataclass(eq=False)
class RelationDefinition:
    to: RelationTarget
    relation_type: str
    foreign_key: str
    owner: type["BaseModel"] | None = field(default=None, init=False, repr=False)
    name: str | None = field(default=None, init=False)
    _resolved_target: type["BaseModel"] | None = field(default=None, init=False, repr=False)

    def bind(self, owner: type["BaseModel"], name: str) -> None:
        if self.owner is not None and self.owner is not owner:
            raise ValueError(
                f"RelationDefinition '{self.name}' is already bound to "
                f"{self.owner.__name__}, cannot rebind to {owner.__name__}"
            )
        self.owner = owner
        self.name = name

    def resolve_target(self) -> type["BaseModel"]:
        if self._resolved_target is not None:
            return self._resolved_target
        if isinstance(self.to, type):
            self._resolved_target = self.to
            return self.to
        if callable(self.to) and not isinstance(self.to, str):
            target = self.to()
            self._resolved_target = target
            return target
        if isinstance(self.to, str):
            from ormrm.models import BaseModel

            target = BaseModel.get_registered_model(self.to)
            self._resolved_target = target
            return target
        return self.to

    @property
    def bound_name(self) -> str:
        if self.name is None:
            raise ValueError("RelationDefinition is not bound to a model")
        return self.name


def define_field(
    *,
    primary_key: bool = False,
    filters: list[type[BaseFilter]] | tuple[type[BaseFilter], ...] | None = None,
    sortable: bool = False,
) -> FieldDefinition:
    return FieldDefinition(
        primary_key=primary_key,
        filters=tuple(filters or ()),
        sortable=sortable,
    )


def create_relation(
    *,
    to: RelationTarget,
    relation_type: str,
    foreign_key: str,
) -> RelationDefinition:
    return RelationDefinition(to=to, relation_type=relation_type, foreign_key=foreign_key)
