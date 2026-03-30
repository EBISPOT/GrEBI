
# The GrEBI datamodel

Data to load into GrEBI should be structured as JSON objects, one per line. Each JSON line represents a node, and its properties are the properties on the node.

GrEBI does not have opinions about the schema of your data, and it doesn't have much of a schema itself apart from a few reserved `grebi:` properties for reification. Most of the data loaded in GrEBI comes from sources with their own schema and validation, for example ontologies using OWL2 or KGs using [LinkML](https://linkml.io/). GrEBI is just the querying engine and does not do any validation.

Each JSON object should have one or more identifier defined using one of the properties in `identifier_props`, configurable by graph; `id` is conventionally used, but the `ebi_monarch_xspecies` graph also adds `skos:exactMatch` and `semapv:crossSpeciesExactMatch` in its config file [ebi_monarch_xspecies.json](https://github.com/EBISPOT/GrEBI/blob/dev/configs/subgraph_configs/ebi_monarch_xspecies.json). All JSON objects which have any identifiers in common will be merged into the same graph node.

## Edges

Edges are created when a property value on one node is an identifier for another node. For example, the following JSON results in both `mondo:0005015` having a property `biolink:has_phenotype hp:0003074`, and a `biolink:has_phenotype` edge being created between `mondo:0005015` and `hp:0003074`.

```json
{ "id": "hp:0003074", "dcterms:title": "Hyperglycemia" }
{ "id": "mondo:0005015", "dcterms:title": "diabetes mellitus", "biolink:has_phenotype": "hp:0003074" }
```

> *Caveat:* Because edges in GrEBI are derived from properties, an edge A->B cannot be created without an associated property in the JSON of node A. This means that you while edges scale easily to millions of _incoming_ edges e.g. gene to associated phenotypes, they do not scale to millions of _outgoing_ edges e.g. phenotype to all the genes, because it would require millions of properties in a JSON line. Because edges in GrEBI can be traversed in either direction this is not a limitation in practice, but incoming edges should be preferred for very large numbers of relationships. Fortunately this tends to be how relationships are modelled already.

## Reification

GrEBI is a property graph where properties can have their own properties, and edges can also have properties. The special `grebi:value` and `grebi:properties` properties are reserved for this purpose. For example, this example JSON results in the `biolink:has_phenotype` property and edge both having the metadata `biolink:xref  https://www.who.int/health-topics/diabetes`. Note that the JSON is shown over multiple lines for readability, but in practice would be a single line.

```json
{
    "id": "mondo:0005015",
    "dcterms:title": "diabetes mellitus",
    "biolink:has_phenotype": {
        "grebi:value": "hp:0003074",
        "grebi:properties": {
            "biolink:xref": "https://www.who.int/health-topics/diabetes"
        }
    }
}
```

 




