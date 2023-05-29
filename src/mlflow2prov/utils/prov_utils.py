from typing import Sequence

import prov.model


def document_factory(
    records: Sequence[prov.model.ProvRecord] | None = None,
) -> prov.model.ProvDocument:
    doc = prov.model.ProvDocument(records=records)
    doc.set_default_namespace(uri="https://github.com/mariusschlegel/mlflow2prov/")

    return doc


def qualified_name(localpart: str) -> prov.model.QualifiedName:
    namespace = document_factory().get_default_namespace()

    return prov.model.QualifiedName(namespace=namespace, localpart=localpart)
