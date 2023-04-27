from mlflow2prov.adapters.git.fetcher import GitFetcher
from mlflow2prov.adapters.mlflow.fetcher import MLflowFetcher
from mlflow2prov.dependencies import Dependencies
from mlflow2prov.service_layer.unit_of_work import InMemoryUnitOfWork


class TestDependencies:
    def test_dependencies_initialization(self):
        deps = Dependencies(
            uow=InMemoryUnitOfWork(),
            git_fetcher=GitFetcher(),
            mlflow_fetcher=MLflowFetcher(),
        )

        assert deps.uow == InMemoryUnitOfWork()
        assert deps.git_fetcher == GitFetcher()
        assert deps.mlflow_fetcher == MLflowFetcher()
