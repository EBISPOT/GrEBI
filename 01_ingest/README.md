
# Ingest format

Because the pipeline does not define a knowledge representation, it by design allows using completely unqualified identifiers, property names, and edge types alongside more formally specified ones. This will enable ingested databases to iteratively improve their semantics while already being queryable as part of the KG.

While this kind of inconsistency is common in the Neo4j world, tools developed by the semantic web and ontology communities are much stricter. For example using RDF and/or OWL as the ingest format would require [identifiers to be mapped to IRIs](https://en.wikipedia.org/wiki/List_of_unsolved_problems_in_computer_science), and using KGX as the ingest format would [limit the set of possible edge types to the BioLink model](https://github.com/biolink/kgx/blob/master/specification/kgx-format.md#predicate).

GrEBI therefore has its own simple intermediate [JSONL](https://jsonlines.org/) property graph format for ingests. Both RDF and KGX can be converted to this format using the provided ingest scripts, and so can various EBI databases. The format is defined solely for use in the intermediate stages of this pipeline and is not intended for users, who instead will access GrEBI through one of the database exports.

Each line of the JSONL is as follows:

```json
{
    "subject": "SUBJECT_ID",
    "datasource": "DATASOURCE_NAME",
    "properties": {
        "PROPERTY_NAME": [ PROPERTY_VALUE, ...  ]
    }
}
```

`DATASOURCE_NAME` is a user-defined name for the datasource to distinguish it from others in the knowledge graph, e.g. "GWAS" or "Reactome".

Each `PROPERTY_VALUE` is a JSON entity representing the property value (string, object, array, or any nested combination of the above).

For reification (i.e. properties of properties, annotations on annotations, or whatever you call it in your world) a `PROPERTY_VALUE` can be a nested object of the type `{"value": ..., "properties": ...}`:

```json
{
    "subject": "SUBJECT_ID",
    "datasource": "DATASOURCE_NAME",
    "properties": {
        "PROPERTY_NAME": [
            {
                "value": PROPERTY_VALUE,
                "properties": {
                    "PROPERTY_NAME": [ PROPERTY_VALUE, ...  ]
                }
            }
        ]
    }
}
```

There is no special representation for edges; they are simply property values where the value is the subject of another node. The pipeline works out when to turn properties into edges. Annotations on edges are implemented as reifications on the values (as above).