import hashlib
import io
import pathlib
import tempfile

import prov.model
import pytest

from mlflow2prov.prov.operations import (
    DeserializationFormat,
    SerializationFormat,
    StatisticsFormat,
    StatisticsResolution,
    build_inverse_index,
    dedupe,
    deserialize,
    format_stats_as_ascii_table,
    format_stats_as_csv,
    get_attribute,
    merge,
    merge_duplicated_agents,
    pseudonymize,
    read_duplicated_agent_mapping,
    read_prov_file,
    serialize,
    statistics,
    uncover_name,
    write_prov_file,
)
from mlflow2prov.utils.prov_utils import document_factory, qualified_name
from tests.utils import random_suffix


class TestSerializationFormat:
    def test_str(self):
        str_serialization_format_json = SerializationFormat.JSON

        assert str(str_serialization_format_json) == "json"
        assert str_serialization_format_json.value == "json"

        str_serialization_format_xml = SerializationFormat.XML

        assert str(str_serialization_format_xml) == "xml"
        assert str_serialization_format_xml.value == "xml"

        str_serialization_format_rdf = SerializationFormat.RDF

        assert str(str_serialization_format_rdf) == "rdf"
        assert str_serialization_format_rdf.value == "rdf"

        str_serialization_format_provn = SerializationFormat.PROVN

        assert str(str_serialization_format_provn) == "provn"
        assert str_serialization_format_provn.value == "provn"

        str_serialization_format_dot = SerializationFormat.DOT

        assert str(str_serialization_format_dot) == "dot"
        assert str_serialization_format_dot.value == "dot"

    def test_from_string(self):
        str_serialization_format_json_1 = "json"
        str_serialization_format_json_2 = "Json"
        str_serialization_format_json_3 = "JSON"

        assert (
            SerializationFormat.from_string(str_serialization_format_json_1)
            == SerializationFormat.JSON
        )
        assert (
            SerializationFormat.from_string(str_serialization_format_json_2)
            == SerializationFormat.JSON
        )
        assert (
            SerializationFormat.from_string(str_serialization_format_json_3)
            == SerializationFormat.JSON
        )

        str_serialization_format_xml_1 = "xml"
        str_serialization_format_xml_2 = "Xml"
        str_serialization_format_xml_3 = "XML"

        assert (
            SerializationFormat.from_string(str_serialization_format_xml_1)
            == SerializationFormat.XML
        )
        assert (
            SerializationFormat.from_string(str_serialization_format_xml_2)
            == SerializationFormat.XML
        )
        assert (
            SerializationFormat.from_string(str_serialization_format_xml_3)
            == SerializationFormat.XML
        )

        str_serialization_format_rdf_1 = "rdf"
        str_serialization_format_rdf_2 = "Rdf"
        str_serialization_format_rdf_3 = "RDF"

        assert (
            SerializationFormat.from_string(str_serialization_format_rdf_1)
            == SerializationFormat.RDF
        )
        assert (
            SerializationFormat.from_string(str_serialization_format_rdf_2)
            == SerializationFormat.RDF
        )
        assert (
            SerializationFormat.from_string(str_serialization_format_rdf_3)
            == SerializationFormat.RDF
        )

        str_serialization_format_provn_1 = "provn"
        str_serialization_format_provn_2 = "Provn"
        str_serialization_format_provn_3 = "PROVN"

        assert (
            SerializationFormat.from_string(str_serialization_format_provn_1)
            == SerializationFormat.PROVN
        )
        assert (
            SerializationFormat.from_string(str_serialization_format_provn_2)
            == SerializationFormat.PROVN
        )
        assert (
            SerializationFormat.from_string(str_serialization_format_provn_3)
            == SerializationFormat.PROVN
        )

        str_serialization_format_dot_1 = "dot"
        str_serialization_format_dot_2 = "Dot"
        str_serialization_format_dot_3 = "DOT"

        assert (
            SerializationFormat.from_string(str_serialization_format_dot_1)
            == SerializationFormat.DOT
        )
        assert (
            SerializationFormat.from_string(str_serialization_format_dot_2)
            == SerializationFormat.DOT
        )
        assert (
            SerializationFormat.from_string(str_serialization_format_dot_3)
            == SerializationFormat.DOT
        )

    def test_to_string(self):
        assert SerializationFormat.to_string(SerializationFormat.JSON) == str(
            SerializationFormat.JSON
        )
        assert SerializationFormat.to_string(SerializationFormat.XML) == str(
            SerializationFormat.XML
        )
        assert SerializationFormat.to_string(SerializationFormat.RDF) == str(
            SerializationFormat.RDF
        )
        assert SerializationFormat.to_string(SerializationFormat.PROVN) == str(
            SerializationFormat.PROVN
        )
        assert SerializationFormat.to_string(SerializationFormat.DOT) == str(
            SerializationFormat.DOT
        )


class TestDeserializationFormat:
    def test_str(self):
        str_deserialization_format_json = DeserializationFormat.JSON

        assert str(str_deserialization_format_json) == "json"
        assert str_deserialization_format_json.value == "json"

        str_deserialization_format_xml = DeserializationFormat.XML

        assert str(str_deserialization_format_xml) == "xml"
        assert str_deserialization_format_xml.value == "xml"

        str_deserialization_format_rdf = DeserializationFormat.RDF

        assert str(str_deserialization_format_rdf) == "rdf"
        assert str_deserialization_format_rdf.value == "rdf"

    def test_from_string(self):
        str_deserialization_format_json_1 = "json"
        str_deserialization_format_json_2 = "Json"
        str_deserialization_format_json_3 = "JSON"

        assert (
            DeserializationFormat.from_string(str_deserialization_format_json_1)
            == DeserializationFormat.JSON
        )
        assert (
            DeserializationFormat.from_string(str_deserialization_format_json_2)
            == DeserializationFormat.JSON
        )
        assert (
            DeserializationFormat.from_string(str_deserialization_format_json_3)
            == DeserializationFormat.JSON
        )

        str_deserialization_format_xml_1 = "xml"
        str_deserialization_format_xml_2 = "Xml"
        str_deserialization_format_xml_3 = "XML"

        assert (
            DeserializationFormat.from_string(str_deserialization_format_xml_1)
            == DeserializationFormat.XML
        )
        assert (
            DeserializationFormat.from_string(str_deserialization_format_xml_2)
            == DeserializationFormat.XML
        )
        assert (
            DeserializationFormat.from_string(str_deserialization_format_xml_3)
            == DeserializationFormat.XML
        )

        str_deserialization_format_rdf_1 = "rdf"
        str_deserialization_format_rdf_2 = "Rdf"
        str_deserialization_format_rdf_3 = "RDF"

        assert (
            DeserializationFormat.from_string(str_deserialization_format_rdf_1)
            == DeserializationFormat.RDF
        )
        assert (
            DeserializationFormat.from_string(str_deserialization_format_rdf_2)
            == DeserializationFormat.RDF
        )
        assert (
            DeserializationFormat.from_string(str_deserialization_format_rdf_3)
            == DeserializationFormat.RDF
        )

    def test_to_string(self):
        assert DeserializationFormat.to_string(DeserializationFormat.JSON) == str(
            DeserializationFormat.JSON
        )
        assert DeserializationFormat.to_string(DeserializationFormat.XML) == str(
            DeserializationFormat.XML
        )
        assert DeserializationFormat.to_string(DeserializationFormat.RDF) == str(
            DeserializationFormat.RDF
        )


class TestStatisticsFormat:
    def test_str(self):
        str_statistics_format_table = StatisticsFormat.TABLE

        assert str(str_statistics_format_table) == "table"
        assert str_statistics_format_table.value == "table"

        str_statistics_format_csv = StatisticsFormat.CSV

        assert str(str_statistics_format_csv) == "csv"
        assert str_statistics_format_csv.value == "csv"

    def test_from_string(self):
        str_statistics_format_table_1 = "table"
        str_statistics_format_table_2 = "Table"
        str_statistics_format_table_3 = "TABLE"

        assert (
            StatisticsFormat.from_string(str_statistics_format_table_1)
            == StatisticsFormat.TABLE
        )
        assert (
            StatisticsFormat.from_string(str_statistics_format_table_2)
            == StatisticsFormat.TABLE
        )
        assert (
            StatisticsFormat.from_string(str_statistics_format_table_3)
            == StatisticsFormat.TABLE
        )

        str_statistics_format_csv_1 = "csv"
        str_statistics_format_csv_2 = "Csv"
        str_statistics_format_csv_3 = "CSV"

        assert (
            StatisticsFormat.from_string(str_statistics_format_csv_1)
            == StatisticsFormat.CSV
        )
        assert (
            StatisticsFormat.from_string(str_statistics_format_csv_2)
            == StatisticsFormat.CSV
        )
        assert (
            StatisticsFormat.from_string(str_statistics_format_csv_3)
            == StatisticsFormat.CSV
        )

    def test_to_string(self):
        assert StatisticsFormat.to_string(StatisticsFormat.TABLE) == str(
            StatisticsFormat.TABLE
        )
        assert StatisticsFormat.to_string(StatisticsFormat.CSV) == str(
            StatisticsFormat.CSV
        )


class TestStatisticsResolution:
    def test_str(self):
        str_statistics_resolution_coarse = StatisticsResolution.COARSE

        assert str(str_statistics_resolution_coarse) == "coarse"
        assert str_statistics_resolution_coarse.value == "coarse"

        str_statistics_resolution_fine = StatisticsResolution.FINE

        assert str(str_statistics_resolution_fine) == "fine"
        assert str_statistics_resolution_fine.value == "fine"

    def test_from_string(self):
        str_statistics_resolution_coarse_1 = "coarse"
        str_statistics_resolution_coarse_2 = "Coarse"
        str_statistics_resolution_coarse_3 = "COARSE"

        assert (
            StatisticsResolution.from_string(str_statistics_resolution_coarse_1)
            == StatisticsResolution.COARSE
        )
        assert (
            StatisticsResolution.from_string(str_statistics_resolution_coarse_2)
            == StatisticsResolution.COARSE
        )
        assert (
            StatisticsResolution.from_string(str_statistics_resolution_coarse_3)
            == StatisticsResolution.COARSE
        )

        str_statistics_resolution_fine_1 = "fine"
        str_statistics_resolution_fine_2 = "Fine"
        str_statistics_resolution_fine_3 = "FINE"

        assert (
            StatisticsResolution.from_string(str_statistics_resolution_fine_1)
            == StatisticsResolution.FINE
        )
        assert (
            StatisticsResolution.from_string(str_statistics_resolution_fine_2)
            == StatisticsResolution.FINE
        )
        assert (
            StatisticsResolution.from_string(str_statistics_resolution_fine_3)
            == StatisticsResolution.FINE
        )

    def test_to_string(self):
        assert StatisticsResolution.to_string(StatisticsResolution.COARSE) == str(
            StatisticsResolution.COARSE
        )
        assert StatisticsResolution.to_string(StatisticsResolution.FINE) == str(
            StatisticsResolution.FINE
        )


class TestOperations:
    def test_serialize(self):
        doc = prov.model.ProvDocument()

        result = serialize(
            document=doc,
            format=SerializationFormat.JSON,
        )
        result_expected = "{}"
        assert result == result_expected

        result = serialize(
            document=doc,
            format=SerializationFormat.DOT,
        )
        result_expected = 'digraph G {\ncharset="utf-8";\nrankdir=BT;\n}\n'
        assert result == result_expected

        result = serialize(
            document=doc,
            format=SerializationFormat.XML,
        )
        result_expected = '<?xml version=\'1.0\' encoding=\'ASCII\'?>\n<prov:document xmlns:prov="http://www.w3.org/ns/prov#" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"/>\n'
        assert result == result_expected

    def test_write_prov_file(self, capfd):
        doc = prov.model.ProvDocument()
        content_expected = serialize(
            document=doc,
            format=SerializationFormat.JSON,
        )

        write_prov_file(
            document=doc,
            filename="-",
            format=SerializationFormat.JSON,
            overwrite=False,
        )
        out, _ = capfd.readouterr()
        assert out == content_expected

        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as tmpfile:
            with pytest.raises(FileExistsError):
                write_prov_file(
                    document=doc,
                    filename=tmpfile.name,
                    format=SerializationFormat.JSON,
                    overwrite=False,
                )

            write_prov_file(
                document=doc,
                filename=tmpfile.name,
                format=SerializationFormat.JSON,
                overwrite=True,
            )

            with open(tmpfile.name, "r") as f:
                content = f.read()
                assert content == content_expected

    def test_deserialize(self):
        content_xml = '<?xml version="1.0" encoding="ASCII"?>\n<prov:document xmlns:prov="http://www.w3.org/ns/prov#" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"/>'
        doc_expected = prov.model.ProvDocument()

        doc = deserialize(
            content=content_xml,
            format=DeserializationFormat.XML,
        )
        assert doc == doc_expected

        doc = deserialize(content=content_xml)
        assert doc == doc_expected

        content_json = 'syntax = "proto3";'
        with pytest.raises(Exception):
            deserialize(content=content_json)

    def test_read_prov_file(self, monkeypatch):
        content_xml = '<?xml version="1.0" encoding="ASCII"?>\n<prov:document xmlns:prov="http://www.w3.org/ns/prov#" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"/>'
        doc_expected = prov.model.ProvDocument()

        monkeypatch.setattr("sys.stdin", io.StringIO(content_xml))
        doc = read_prov_file(filename="-")
        assert doc == doc_expected

        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".xml",
        ) as tmpfile:
            tmpfile.write(content_xml)

            doc = read_prov_file(filename=tmpfile.name)
            assert doc == doc_expected

    def test_format_as_ascii_table(self):
        d = {"A": 1, "B": 2, "C": 3}
        header_expected = [
            f"|{'Record Type':20}|{'Count':20}|",
            f"+{'-'*20}+{'-'*20}+",
        ]
        body_expected = [
            f"|{'A':20}|{1:20}|",
            f"|{'B':20}|{2:20}|",
            f"|{'C':20}|{3:20}|",
        ]
        table = format_stats_as_ascii_table(d)
        lines = [l.strip() for l in table.split("\n") if l]
        assert lines[:2] == header_expected
        assert lines[2:] == body_expected

    def test_format_stats_as_csv(self):
        d = {"A": 1, "B": 2, "C": 3}
        header_expected = ["Record Type, Count"]
        body_expected = [
            "A, 1",
            "B, 2",
            "C, 3",
        ]
        csv = format_stats_as_csv(d)
        lines = [l.strip() for l in csv.split("\n") if l]
        assert lines[:1] == header_expected
        assert lines[1:] == body_expected

    def test_statistics(self):
        graph = document_factory()
        agent = graph.agent(qualified_name(f"agent-id-{random_suffix()}"))
        entity = graph.entity(qualified_name(f"entity-id-{random_suffix()}"))
        r1 = graph.wasAttributedTo(entity, agent)
        r2 = graph.wasAttributedTo(entity, agent)

        result = statistics(graph=graph)
        assert (
            result
            == "|Record Type         |Count               |\n+--------------------+--------------------+\n|Agent               |                   1|\n|Entity              |                   1|\n|Relations           |                   2|\n"
        )

        result = statistics(graph=graph, resolution=StatisticsResolution.FINE)
        assert (
            result
            == "|Record Type         |Count               |\n+--------------------+--------------------+\n|Agent               |                   1|\n|Entity              |                   1|\n|Attribution         |                   2|\n"
        )

        result = statistics(graph=graph, format=StatisticsFormat.CSV)
        assert result == "Record Type, Count\nAgent, 1\nEntity, 1\nRelations, 2\n"

    def test_merge_returns_empty_graph_when_run_wo_subgraphs(self):
        assert merge([]) == document_factory()

    def test_merge_carries_over_all_records(self):
        agent1 = prov.model.ProvAgent(
            None, qualified_name(f"agent-id-{random_suffix()}")
        )
        agent2 = prov.model.ProvAgent(
            None, qualified_name(f"agent-id-{random_suffix()}")
        )
        graph1 = prov.model.ProvDocument([agent1])
        graph2 = prov.model.ProvDocument([agent2])
        subgraphs = [graph1, graph2]
        expected_graph = prov.model.ProvDocument([agent1, agent2])
        assert merge(subgraphs) == expected_graph

    def test_dedupe_removes_duplicate_elements(self):
        agent = prov.model.ProvAgent(
            None, qualified_name(f"agent-id-{random_suffix()}")
        )
        graph = prov.model.ProvDocument([agent, agent])
        expected_graph = prov.model.ProvDocument([agent])
        assert list(graph.get_records(prov.model.ProvAgent)) == [agent, agent]
        assert list(dedupe(graph).get_records(prov.model.ProvAgent)) == [agent]
        assert dedupe(graph) == expected_graph

    def test_dedupe_merges_attributes_of_duplicate_elements(self):
        id = qualified_name(f"agent-id-{random_suffix()}")
        graph = prov.model.ProvDocument()
        graph.agent(id, {"attribute1": 1})
        graph.agent(id, {"attribute2": 2})
        expected_attributes = [
            (qualified_name("attribute1"), 1),
            (qualified_name("attribute2"), 2),
        ]
        agents = list(dedupe(graph).get_records(prov.model.ProvAgent))
        assert len(agents) == 1
        assert agents[0].attributes == expected_attributes

    def test_dedupe_remove_duplicate_relations(self):
        graph = prov.model.ProvDocument()
        agent = graph.agent(qualified_name(f"agent-id-{random_suffix()}"))
        entity = graph.entity(qualified_name(f"entity-id-{random_suffix()}"))
        r1 = graph.wasAttributedTo(entity, agent)
        r2 = graph.wasAttributedTo(entity, agent)
        assert list(graph.get_records(prov.model.ProvRelation)) == [r1, r2]
        assert list(dedupe(graph).get_records(prov.model.ProvRelation)) == [r1]

    def test_dedupe_merges_attributes_of_duplicate_relations(self):
        graph = prov.model.ProvDocument()
        agent = graph.agent(qualified_name(f"agent-id-{random_suffix()}"))
        entity = graph.entity(qualified_name(f"entity-id-{random_suffix()}"))
        r1_attrs = [(qualified_name("attr"), "val1")]
        r2_attrs = [(qualified_name("attr"), "val2")]
        graph.wasAttributedTo(entity, agent, other_attributes=r1_attrs)
        graph.wasAttributedTo(entity, agent, other_attributes=r2_attrs)

        graph = dedupe(graph)

        relations = list(graph.get_records(prov.model.ProvRelation))
        assert len(relations) == 1
        expected_extra_attributes = set(
            [
                (qualified_name("attr"), "val1"),
                (qualified_name("attr"), "val2"),
            ]
        )
        assert set(relations[0].extra_attributes) == expected_extra_attributes

    def test_read_duplicated_agent_mapping_and_build_inverse_index(self):
        content_agent_mapping = """
        - name: "Foo Bar"
          aliases: ["foo", "bar", "foobar", "root"]
        """

        with tempfile.NamedTemporaryFile(mode="r+", encoding="utf-8") as tmpfile:
            tmpfile.write(content_agent_mapping)
            tmpfile.seek(0)

            result = read_duplicated_agent_mapping(fp=pathlib.Path(tmpfile.name))
            result_expected = {"Foo Bar": ["foo", "bar", "foobar", "root"]}
            assert result == result_expected

        content_agent_mapping_empty = ""

        with tempfile.NamedTemporaryFile(mode="r+", encoding="utf-8") as tmpfile:
            tmpfile.write(content_agent_mapping_empty)
            tmpfile.seek(0)

            result = read_duplicated_agent_mapping(fp=pathlib.Path(tmpfile.name))
            result_expected = dict()
            assert result == result_expected

    def test_build_inverse_index(self):
        mapping = {"name": ["alias1", "alias2"]}
        expected_dict = {"alias1": "name", "alias2": "name"}
        assert build_inverse_index(mapping) == expected_dict

    def test_uncover_name(self):
        names = {"alias": "name"}
        graph = document_factory()
        agent = graph.agent(
            "agent-id", other_attributes={qualified_name("name"): "alias"}
        )

        expected_name = (qualified_name("name"), "name")
        assert uncover_name(agent, names) == expected_name

    def test_uncover_duplicated_agents_resolves_agent_alias(self, mocker):
        d = {"alias1": "name", "alias2": "name"}
        mocker.patch("mlflow2prov.prov.operations.read_duplicated_agent_mapping")
        mocker.patch("mlflow2prov.prov.operations.build_inverse_index", return_value=d)

        graph = document_factory()
        graph.agent("agent1", {"name": "alias2"})
        graph.agent("agent2", {"name": "alias1"})

        graph = merge_duplicated_agents(graph=graph, path_to_mapping=None)

        agents = list(graph.get_records(prov.model.ProvAgent))
        assert len(agents) == 1

        expected_name = "name"
        [(_, name)] = [(k, v) for k, v in agents[0].attributes if k.localpart == "name"]
        assert name == expected_name

    def test_uncover_duplicated_agents_reroutes_relations(self, mocker):
        d = {"alias1": "name", "alias2": "name"}
        mocker.patch("mlflow2prov.prov.operations.read_duplicated_agent_mapping")
        mocker.patch("mlflow2prov.prov.operations.build_inverse_index", return_value=d)

        graph = document_factory()
        a1 = graph.agent("agent1", {"name": "alias2"})
        a2 = graph.agent("agent2", {"name": "alias1"})
        e1 = graph.entity("entity1")
        e2 = graph.entity("entity2")
        e1.wasAttributedTo(a1)
        e2.wasAttributedTo(a2)

        graph = merge_duplicated_agents(graph=graph, path_to_mapping=None)

        relations = list(graph.get_records(prov.model.ProvRelation))
        assert len(relations) == 2
        expected_identifier = "User?name=name"
        assert all(
            relation.formal_attributes[1][1].localpart == expected_identifier
            for relation in relations
        )

    def test_get_attribute_return_none(self):
        graph = document_factory()
        name = f"agent-name-{random_suffix()}"
        email = f"agent-email-{random_suffix()}"
        agent = graph.agent("agent1", {"name": name, "email": email})

        assert get_attribute(agent, "mobile") == None

    def test_pseudonymize_user_agent_without_name(self):
        graph = document_factory()
        email = f"agent-email-{random_suffix()}"
        graph.agent("agent1", {"email": email})

        with pytest.raises(ValueError):
            graph = pseudonymize(graph)

    def test_pseudonymize_changes_agent_name_and_identifier(self):
        graph = document_factory()
        name = f"agent-name-{random_suffix()}"
        email = f"agent-email-{random_suffix()}"
        graph.agent("agent1", {"name": name, "email": email})

        graph = pseudonymize(graph)

        expected_name = hashlib.sha256(bytes(name, "utf-8")).hexdigest()
        expected_email = hashlib.sha256(bytes(email, "utf-8")).hexdigest()
        expected_identifier = qualified_name(
            f"User?name={expected_name}&email={expected_email}"
        )

        agent = list(graph.get_records(prov.model.ProvAgent))[0]
        assert agent.identifier == expected_identifier
        assert list(agent.get_attribute("name"))[0] == expected_name
        assert list(agent.get_attribute("email"))[0] == expected_email

    def test_pseudonymize_deletes_non_name_attributes_apart_from_role_and_type(self):
        graph = document_factory()
        agent_name = f"agent-name-{random_suffix()}"
        a1 = graph.agent(
            "agent1",
            {
                "name": agent_name,
                "email": f"email-{random_suffix()}",
                "mastodon-username": f"mastodon-username-{random_suffix()}",
                prov.model.PROV_ROLE: f"prov-role-{random_suffix()}",
                prov.model.PROV_TYPE: f"prov-type-{random_suffix()}",
            },
        )
        e1 = graph.entity("entity1")
        graph.wasAttributedTo(
            agent=a1, entity=e1, other_attributes=[("attributed-to-agent", agent_name)]
        )

        graph = pseudonymize(graph)

        agent = list(graph.get_records(prov.model.ProvAgent))[0]
        expected_attributes = [
            prov.model.PROV_ROLE,
            prov.model.PROV_TYPE,
            qualified_name("name"),
            qualified_name("email"),
        ]
        assert all(
            [(attr in expected_attributes) for (attr, _) in agent.extra_attributes]
        )
