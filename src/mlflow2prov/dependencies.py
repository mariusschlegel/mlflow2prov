from dataclasses import dataclass, field

from mlflow2prov.adapters.git.fetcher import GitFetcher
from mlflow2prov.adapters.mlflow.fetcher import MLflowFetcher
from mlflow2prov.service_layer.unit_of_work import InMemoryUnitOfWork


@dataclass
class Dependencies:
    uow: InMemoryUnitOfWork = field(default_factory=InMemoryUnitOfWork)
    git_fetcher: GitFetcher = field(default_factory=GitFetcher)
    mlflow_fetcher: MLflowFetcher = field(default_factory=MLflowFetcher)
