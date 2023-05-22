from __future__ import annotations

import hashlib
import logging
import pathlib
import sys
from collections import Counter, defaultdict
from contextlib import suppress
from enum import Enum
from typing import Any, NamedTuple, Type
from urllib.parse import urlencode

import prov.dot
import prov.identifier
import prov.model
import ruamel.yaml

from mlflow2prov.utils.prov_utils import document_factory, qualified_name

log = logging.getLogger(__name__)


class SerializationFormat(Enum):
    JSON = "json"
    XML = "xml"
    RDF = "rdf"
    PROVN = "provn"
    DOT = "dot"

    def __str__(self) -> str:
        return self.value

    @staticmethod
    def from_string(format_str: str) -> SerializationFormat:
        if format_str in ("json", "Json", "JSON"):
            return SerializationFormat.JSON
        elif format_str in ("xml", "Xml", "XML"):
            return SerializationFormat.XML
        elif format_str in ("rdf", "Rdf", "RDF"):
            return SerializationFormat.RDF
        elif format_str in ("provn", "Provn", "PROVN"):
            return SerializationFormat.PROVN
        elif format_str in ("dot", "Dot", "DOT"):
            return SerializationFormat.DOT
        else:
            raise NotImplementedError

    @staticmethod
    def to_string(format: SerializationFormat) -> str:
        return str(format)

    @classmethod
    def values(cls):
        return list(cls._value2member_map_.keys())


class DeserializationFormat(Enum):
    JSON = "json"
    XML = "xml"
    RDF = "rdf"

    def __str__(self) -> str:
        return self.value

    @staticmethod
    def from_string(format_str: str) -> DeserializationFormat:
        if format_str in ("json", "Json", "JSON"):
            return DeserializationFormat.JSON
        elif format_str in ("xml", "Xml", "XML"):
            return DeserializationFormat.XML
        elif format_str in ("rdf", "Rdf", "RDF"):
            return DeserializationFormat.RDF
        else:
            raise NotImplementedError

    @staticmethod
    def to_string(format: DeserializationFormat) -> str:
        return str(format)

    @classmethod
    def values(cls) -> list[DeserializationFormat]:
        return list(cls._value2member_map_.keys())


def serialize(
    document: prov.model.ProvDocument,
    format: SerializationFormat = SerializationFormat.JSON,
) -> str | None:
    if format != SerializationFormat.DOT:
        # format == "json" or
        # format == "xml" or
        # format == "rdf" or
        # format == "provn"
        return document.serialize(format=SerializationFormat.to_string(format))
    else:
        # format == "dot"
        return prov.dot.prov_to_dot(bundle=document).to_string()


def write_prov_file(
    document: prov.model.ProvDocument,
    filename: str,
    format: SerializationFormat = SerializationFormat.JSON,
    overwrite: bool = True,
) -> None:
    if filename == "-":
        content = serialize(document, format=format)
        if content:
            content = sys.stdout.write(content)
    else:
        if pathlib.Path(filename).exists() and not overwrite:
            raise FileExistsError(f"File {filename} already exists.")
        with open(filename, "w") as f:
            content = serialize(document=document, format=format)
            if content:
                f.write(content)


def deserialize(
    content: str,
    format: DeserializationFormat | None = None,
) -> prov.model.ProvDocument | None:
    if format == None:
        formats = DeserializationFormat.values()
    else:
        formats = [format]

    for fmt in formats:
        with suppress(Exception):
            doc = prov.model.ProvDocument.deserialize(
                content=content, format=DeserializationFormat.to_string(fmt)
            )
            return doc

    raise Exception(f"Deserialization failed for {content=}")


def read_prov_file(
    filename: str,
    format: DeserializationFormat | None = None,
) -> prov.model.ProvDocument | None:
    if filename == "-":
        content = sys.stdin.read()
    else:
        with open(filename, "r") as f:
            content = f.read()
    return deserialize(content=content, format=format)


def format_stats_as_ascii_table(stats: dict[str, int]) -> str:
    table = f"|{'Record Type':20}|{'Count':20}|\n+{'-'*20}+{'-'*20}+\n"
    for record_type, count in stats.items():
        table += f"|{record_type:20}|{count:20}|\n"
    return table


def format_stats_as_csv(stats: dict[str, int]) -> str:
    csv = f"Record Type, Count\n"
    for record_type, count in stats.items():
        csv += f"{record_type}, {count}\n"
    return csv


class StatisticsFormat(Enum):
    TABLE = "table"
    CSV = "csv"

    def __str__(self) -> str:
        return self.value

    @staticmethod
    def from_string(format_str: str) -> StatisticsFormat:
        if format_str in ("table", "Table", "TABLE"):
            return StatisticsFormat.TABLE
        elif format_str in ("csv", "Csv", "CSV"):
            return StatisticsFormat.CSV
        else:
            raise NotImplementedError

    @staticmethod
    def to_string(format: StatisticsFormat) -> str:
        return str(format)

    @classmethod
    def values(cls):
        return list(cls._value2member_map_.keys())


class StatisticsResolution(Enum):
    COARSE = "coarse"
    FINE = "fine"

    def __str__(self) -> str:
        return self.value

    @staticmethod
    def from_string(format_str: str) -> StatisticsResolution:
        if format_str in ("coarse", "Coarse", "COARSE"):
            return StatisticsResolution.COARSE
        elif format_str in ("fine", "Fine", "FINE"):
            return StatisticsResolution.FINE
        else:
            raise NotImplementedError

    @staticmethod
    def to_string(format: StatisticsResolution) -> str:
        return str(format)

    @classmethod
    def values(cls):
        return list(cls._value2member_map_.keys())


def statistics(
    graph: prov.model.ProvDocument,
    resolution: StatisticsResolution = StatisticsResolution.COARSE,
    format: StatisticsFormat = StatisticsFormat.TABLE,
) -> str:
    if format == StatisticsFormat.CSV:
        formatter = format_stats_as_csv
    else:
        # format == "table"
        formatter = format_stats_as_ascii_table

    elements = Counter(
        e.get_type().localpart for e in graph.get_records(prov.model.ProvElement)
    )
    relations = Counter(
        r.get_type().localpart for r in graph.get_records(prov.model.ProvRelation)
    )

    stats = dict(sorted(elements.items()))
    if resolution == StatisticsResolution.FINE:
        stats.update(sorted(relations.items()))
    else:
        # resolution == StatisticsResolution.COARSE
        stats.update({"Relations": relations.total()})

    return formatter(stats)


def merge(graphs: list[prov.model.ProvDocument]) -> prov.model.ProvDocument:
    if graphs != []:
        acc = graphs[0]
    else:
        acc = prov.model.ProvDocument()

    for graph in graphs:
        acc.update(graph)

    return dedupe(acc)


class StrippedRelation(NamedTuple):
    s: prov.model.QualifiedName
    t: prov.model.QualifiedName
    type: Type[prov.model.ProvRelation]


def dedupe(graph: prov.model.ProvDocument) -> prov.model.ProvDocument:
    graph = graph.unified()
    records = list(graph.get_records((prov.model.ProvElement)))

    bundles = dict()
    attributes = defaultdict(set)

    for relation in graph.get_records(prov.model.ProvRelation):
        stripped = StrippedRelation(
            relation.formal_attributes[0],
            relation.formal_attributes[1],
            prov.model.PROV_REC_CLS[relation.get_type()],
        )
        bundles[stripped] = relation.bundle
        attributes[stripped].update(relation.extra_attributes)

    records.extend(
        relation.type(
            bundles[relation],
            None,
            [relation.s, relation.t] + list(attributes[relation]),
        )
        for relation in attributes
    )

    return document_factory(records)


def read_duplicated_agent_mapping(fp: pathlib.Path) -> dict[str, list[str]]:
    with open(fp, "rt") as f:
        yaml = ruamel.yaml.YAML(typ="safe")
        agents = yaml.load(f.read())

    if agents:
        return {agent["name"]: agent["aliases"] for agent in agents}
    else:
        return dict()


def build_inverse_index(mapping: dict[str, list[str]]) -> dict[str, str]:
    return {alias: name for name, aliases in mapping.items() for alias in aliases}


def uncover_name(
    agent: prov.model.ProvAgent,
    names: dict[str, str],
) -> tuple[prov.model.QualifiedName, str]:
    [(qualified_name, name)] = [
        (key, val) for key, val in agent.attributes if key.localpart == "name"
    ]

    return qualified_name, names.get(name, name)


def merge_duplicated_agents(
    graph: prov.model.ProvDocument,
    path_to_mapping: pathlib.Path | None = None,
) -> prov.model.ProvDocument:
    mapping = (
        read_duplicated_agent_mapping(path_to_mapping) if path_to_mapping else dict()
    )
    names = build_inverse_index(mapping)

    # dict to temporarily store agent attributes
    attrs = defaultdict(set)
    # map of old agent identifiers to new agent identifiers
    # used to reroute relationships
    reroute = dict()
    # prov records that are not affected by this operation
    records = list(graph.get_records((prov.model.ProvEntity, prov.model.ProvActivity)))

    for agent in graph.get_records(prov.model.ProvAgent):
        # resolve the agent alias (uncover its identity)
        name = uncover_name(agent, names)
        # rebuild the attributes of the current agent
        # start by adding the uncovered given name
        attrs[name].add(name)
        # add all other attributes aswell
        attrs[name].update(t for t in agent.attributes if t[0].localpart != "name")

        repr_attrs = [tpl for tpl in attrs[name] if tpl[1] in ("name", "email")]
        identifier = qualified_name(f"User?{urlencode(repr_attrs)}")
        records.append(prov.model.ProvAgent(agent.bundle, identifier, attrs[name]))

        reroute[agent.identifier] = identifier

    for relation in graph.get_records(prov.model.ProvRelation):
        formal = [
            (key, reroute.get(val, val)) for key, val in relation.formal_attributes
        ]
        extra = [(key, reroute.get(val, val)) for key, val in relation.extra_attributes]
        r_type = prov.model.PROV_REC_CLS.get(relation.get_type())
        if r_type:
            records.append(r_type(relation.bundle, relation.identifier, formal + extra))

    return document_factory(records).unified()


def get_attribute(
    record: prov.model.ProvRecord, attribute: str, first: bool = True
) -> list[str] | str | None:
    choices = list(record.get_attribute(attribute))
    if not choices:
        return
    else:
        return choices[0] if first else choices


def pseudonymize_agent(
    agent: prov.model.ProvAgent,
    identifier: prov.model.QualifiedName,
    keep: list[prov.model.QualifiedName],
    replace: dict[str, Any],
) -> prov.model.ProvAgent:
    kept = [(key, val) for key, val in agent.extra_attributes if key in keep]
    replaced = [
        (key, replace.get(key.localpart, val))
        for key, val in agent.extra_attributes
        if key.localpart in replace
    ]
    return prov.model.ProvAgent(agent.bundle, identifier, kept + replaced)


def pseudonymize(graph: prov.model.ProvDocument) -> prov.model.ProvDocument:
    # get all records except for agents and relations
    records = list(graph.get_records((prov.model.ProvActivity, prov.model.ProvEntity)))

    pseudonyms = dict()
    for agent in graph.get_records(prov.model.ProvAgent):
        name = get_attribute(agent, "name")
        mail = get_attribute(agent, "email")

        if name == None:
            raise ValueError("ProvAgent representing a user has to have a name!")

        # hash name and mail if present
        namehash = hashlib.sha256(bytes(str(name), "utf-8")).hexdigest()
        mailhash = (
            hashlib.sha256(bytes(str(mail), "utf-8")).hexdigest() if mail else None
        )
        # create a new id as a pseudonym using the hashes
        pseudonym = qualified_name(f"User?name={namehash}&email={mailhash}")

        # map the old id to the pseudonym
        pseudonyms[agent.identifier] = pseudonym

        # keep only prov role & prov type
        # replace name & mail with hashes
        pseudonymized = pseudonymize_agent(
            agent,
            identifier=pseudonym,
            keep=[prov.model.PROV_ROLE, prov.model.PROV_TYPE],
            replace={"name": namehash, "email": mailhash},
        )
        # add pseudonymized agent to the list of records
        records.append(pseudonymized)

    # replace old id occurences with the pseudonymized id
    for relation in graph.get_records(prov.model.ProvRelation):
        formal = [
            (key, pseudonyms.get(val, val)) for key, val in relation.formal_attributes
        ]
        extra = [
            (key, pseudonyms.get(val, val)) for key, val in relation.extra_attributes
        ]

        r_type = prov.model.PROV_REC_CLS.get(relation.get_type())
        if r_type != None:
            records.append(r_type(relation.bundle, relation.identifier, formal + extra))

    return document_factory(records)
