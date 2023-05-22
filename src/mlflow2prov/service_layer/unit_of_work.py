from __future__ import annotations

import abc
from collections import defaultdict

from mlflow2prov.adapters import repository


class AbstractUnitOfWork(abc.ABC):
    def __enter__(self) -> AbstractUnitOfWork:
        return self

    def __exit__(self, *args):
        self.rollback()

    def commit(self):
        self._commit()

    @abc.abstractmethod
    def _commit(self):
        raise NotImplementedError

    @abc.abstractmethod
    def rollback(self):
        raise NotImplementedError

    @abc.abstractmethod
    def __eq__(self, other):
        raise NotImplementedError


class InMemoryUnitOfWork(AbstractUnitOfWork):
    def __init__(self):
        self.resources = defaultdict(repository.InMemoryRepository)

    def __enter__(self):
        return super().__enter__()

    def __exit__(self, *args):
        super().__exit__(*args)

    def _commit(self):
        pass

    def rollback(self):
        pass

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return self.resources == other.resources
        return False
