from dataclasses import dataclass

from mlflow2prov.adapters.git.fetcher import GitFetcher
from mlflow2prov.adapters.mlflow.fetcher import MLflowFetcher
from mlflow2prov.service_layer.unit_of_work import InMemoryUnitOfWork


@dataclass
class Dependencies:
    uow: InMemoryUnitOfWork = InMemoryUnitOfWork()
    git_fetcher: GitFetcher = GitFetcher()
    mlflow_fetcher: MLflowFetcher = MLflowFetcher()
