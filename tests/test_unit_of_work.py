from mlflow2prov.adapters.repository import InMemoryRepository
from mlflow2prov.service_layer.unit_of_work import InMemoryUnitOfWork


class TestInMemoryUnitOfWork:
    def test_with(self):
        uow = InMemoryUnitOfWork()

        with uow:
            uow.resources["foo"].add("bar")

    def test_commit(self):
        uow = InMemoryUnitOfWork()

        assert uow.commit() == None

    def test_rollback(self):
        uow = InMemoryUnitOfWork()

        assert uow.rollback() == None

    def test_eq(self):
        uow1 = InMemoryUnitOfWork()
        uow2 = InMemoryUnitOfWork()

        assert uow1 == uow2

        assert not uow1 == InMemoryRepository()
