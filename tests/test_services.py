import itertools
import pathlib
import tempfile

import prov.model

from mlflow2prov.adapters.git.fetcher import GitFetcher
from mlflow2prov.adapters.mlflow.fetcher import MLflowFetcher
from mlflow2prov.prov import model, operations
from mlflow2prov.service_layer.services import (
    compile_graph,
    fetch_git_from_path,
    fetch_mlflow,
    merge,
    read,
    statistics,
    transform,
    write,
)
from mlflow2prov.service_layer.unit_of_work import InMemoryUnitOfWork
from mlflow2prov.utils.prov_utils import document_factory, qualified_name
from tests.test_git_fetcher import path_testproject_git_repo
from tests.utils import random_suffix


class TestServices:
    def test_fetch_git_from_path(self):
        git_fetcher = GitFetcher()
        git_fetcher.get_from_local_path(path=path_testproject_git_repo)
        fetched_expected = list(git_fetcher.fetch_all())

        uow = InMemoryUnitOfWork()
        fetch_git_from_path(
            path=path_testproject_git_repo,
            uow=uow,
            git_fetcher=git_fetcher,
        )
        fetched_nested = uow.resources[str(path_testproject_git_repo)].repo.values()
        fetched = list(itertools.chain(*fetched_nested))

        assert fetched == fetched_expected

    def test_fetch_mlflow(self):
        mlflow_fetcher = MLflowFetcher()
        url = str(mlflow_fetcher.tracking_uri)
        fetched_expected = list(mlflow_fetcher.fetch_all())

        uow = InMemoryUnitOfWork()
        fetch_mlflow(
            url=url,
            uow=uow,
            mlflow_fetcher=mlflow_fetcher,
        )
        fetched_nested = uow.resources[url].repo.values()
        fetched = list(itertools.chain(*fetched_nested))

        assert fetched == fetched_expected

    def test_compile_graph(self):
        path = str(path_testproject_git_repo)
        url = str(MLflowFetcher().tracking_uri)
        uow = InMemoryUnitOfWork()
        graph_expected = compile_graph([path, url], uow)

        graph = prov.model.ProvDocument()
        for prov_model in model.MODELS:
            model_result = prov_model([uow.resources[path], uow.resources[url]])
            graph = operations.merge(graphs=[graph, model_result])
            graph = operations.dedupe(graph=graph)

        assert graph == graph_expected

    def test_merge(self):
        agent1 = prov.model.ProvAgent(
            None, qualified_name(f"agent-id-{random_suffix()}")
        )
        agent2 = prov.model.ProvAgent(
            None, qualified_name(f"agent-id-{random_suffix()}")
        )
        graph1 = prov.model.ProvDocument([agent1])
        graph2 = prov.model.ProvDocument([agent2])
        documents = [graph1, graph2]

        assert merge(documents=documents) == operations.merge(graphs=documents)

    def test_transform(self):
        document = prov.model.ProvDocument()

        with tempfile.NamedTemporaryFile(
            mode="r",
            encoding="utf-8",
            suffix=".xml",
        ) as tmpfile:
            graph_expected = transform(
                document=document,
                use_pseudonyms=True,
                eliminate_duplicates=True,
                merge_aliased_agents=pathlib.Path(tmpfile.name),
            )

            graph = operations.pseudonymize(graph=document)
            graph = operations.dedupe(graph=graph)

            assert graph == graph_expected

    def test_write(self):
        document = prov.model.ProvDocument()
        content_expected = operations.serialize(
            document=document,
            format=operations.SerializationFormat.JSON,
        )

        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as tmpfile1:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as tmpfile2:
                format = operations.SerializationFormat.JSON

                assert write(
                    document=document, filename=tmpfile1.name, format=format
                ) == operations.write_prov_file(
                    document=document, filename=tmpfile2.name, format=format
                )

                with open(tmpfile1.name, "r") as f1:
                    with open(tmpfile2.name, "r") as f2:
                        assert f1.read() == f2.read()

    def test_read(self):
        content_xml = '<?xml version="1.0" encoding="ASCII"?>\n<prov:document xmlns:prov="http://www.w3.org/ns/prov#" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"/>'
        doc_expected = prov.model.ProvDocument()
        format = operations.DeserializationFormat.XML

        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".xml",
        ) as tmpfile:
            tmpfile.write(content_xml)
            tmpfile.seek(0)

            assert read(
                filename=tmpfile.name, format=format
            ) == operations.read_prov_file(filename=tmpfile.name, format=format)
            assert read(filename=tmpfile.name, format=format) == doc_expected

    def test_statistics(self):
        graph = document_factory()
        agent = graph.agent(qualified_name(f"agent-id-{random_suffix()}"))
        entity = graph.entity(qualified_name(f"entity-id-{random_suffix()}"))
        graph.wasAttributedTo(entity, agent)
        graph.wasAttributedTo(entity, agent)

        resolution = operations.StatisticsResolution.FINE
        format = operations.StatisticsFormat.TABLE

        assert statistics(
            document=graph, resolution=resolution, format=format
        ) == operations.statistics(graph=graph, resolution=resolution, format=format)
