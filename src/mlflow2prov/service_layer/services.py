import logging
import pathlib

import prov.model

from mlflow2prov.adapters.git.fetcher import GitFetcher
from mlflow2prov.adapters.mlflow.fetcher import MLflowFetcher
from mlflow2prov.prov import model, operations
from mlflow2prov.prov.operations import (
    DeserializationFormat,
    SerializationFormat,
    StatisticsFormat,
    StatisticsResolution,
)
from mlflow2prov.service_layer.unit_of_work import InMemoryUnitOfWork

log = logging.getLogger(__name__)


def fetch_git_from_path(
    path: pathlib.Path,
    uow: InMemoryUnitOfWork,
    git_fetcher: GitFetcher,
) -> None:
    git_fetcher.get_from_local_path(path=path)

    with uow:
        for resource in git_fetcher.fetch_all():
            uow.resources[str(path)].add(resource)

    uow.commit()


def fetch_mlflow(
    url: str,
    uow: InMemoryUnitOfWork,
    mlflow_fetcher: MLflowFetcher,
) -> None:
    mlflow_fetcher.tracking_uri = url

    with uow:
        for resource in mlflow_fetcher.fetch_all():
            uow.resources[url].add(resource)

    uow.commit()


def compile_graph(
    locations: list[str],
    uow: InMemoryUnitOfWork,
) -> prov.model.ProvDocument:
    document = prov.model.ProvDocument()

    for prov_model in model.MODELS:
        model_result = prov_model(
            [uow.resources[locations[0]], uow.resources[locations[1]]]
        )
        document = operations.merge(graphs=[document, model_result])
        document = operations.dedupe(graph=document)

    return document


def transform(
    document: prov.model.ProvDocument,
    use_pseudonyms: bool = False,
    eliminate_duplicates: bool = False,
    merge_aliased_agents: pathlib.Path | None = None,
) -> prov.model.ProvDocument:
    if use_pseudonyms:
        document = operations.pseudonymize(document)
    if eliminate_duplicates:
        document = operations.dedupe(document)
    if merge_aliased_agents != None:
        document = operations.merge_duplicated_agents(
            graph=document, path_to_mapping=merge_aliased_agents
        )

    return document


def merge(documents: list[prov.model.ProvDocument]) -> prov.model.ProvDocument:
    return operations.merge(graphs=documents)


def write(
    document: prov.model.ProvDocument,
    filename: str,
    format: SerializationFormat,
) -> None:
    return operations.write_prov_file(
        document=document, filename=filename, format=format
    )


def read(
    filename: str,
    format: DeserializationFormat | None = None,
) -> prov.model.ProvDocument | None:
    return operations.read_prov_file(filename=filename, format=format)


def statistics(
    document: prov.model.ProvDocument,
    resolution: StatisticsResolution,
    format: operations.StatisticsFormat,
) -> str:
    return operations.statistics(graph=document, resolution=resolution, format=format)
