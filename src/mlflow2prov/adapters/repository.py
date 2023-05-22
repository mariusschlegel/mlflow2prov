import abc
from collections import defaultdict
from typing import Any, Type, TypeVar

R = TypeVar("R")


class AbstractRepository(abc.ABC):
    def add(self, resource: R) -> None:
        self._add(resource)

    def get(self, resource_type: Type[R], **filters: Any) -> R | None:
        resource = self._get(resource_type, **filters)
        return resource

    def list_all(self, resource_type: Type[R], **filters: Any) -> list[R]:
        resources = self._list_all(resource_type, **filters)
        return resources

    @abc.abstractmethod
    def _add(self, resource: R) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def _get(self, resource_type: Type[R], **filters: Any) -> R | None:
        raise NotImplementedError

    @abc.abstractmethod
    def _list_all(self, resource_type: Type[R], **filters: Any) -> list[R]:
        raise NotImplementedError

    @abc.abstractmethod
    def __eq__(self, other):
        raise NotImplementedError


class InMemoryRepository(AbstractRepository):
    def __init__(self):
        super().__init__()
        self.repo = defaultdict(list)

    def _add(self, resource: R) -> None:  # type: ignore
        self.repo[type(resource)].append(resource)

    def _get(self, resource_type: Type[R], **filters: Any) -> R | None:
        return next(
            (
                r
                for r in self.repo.get(resource_type, [])
                if all(getattr(r, key) == val for key, val in filters.items())
            ),
            None,
        )

    def _list_all(self, resource_type: Type[R], **filters: Any) -> list[R]:
        return [
            r
            for r in self.repo.get(resource_type, [])
            if all(getattr(r, key) == val for key, val in filters.items())
        ]

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return self.repo == other.repo
        return False
