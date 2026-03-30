# `grebi_rdf2jsonl`

Converts RDF data (XML, Turtle, N-Quads) into JSONL suitable for loading into GrEBI. Loads the entire graph into memory, resolves OWL axiom and RDF statement reifications, and writes one JSONL record per subject.

## Usage

```
cat input.rdf | grebi_rdf2jsonl --rdf_type <TYPE> [OPTIONS] > output.jsonl
```

## Options

| Option | Required | Description |
|---|---|---|
| `--rdf_type <TYPE>` | **Yes** | Input format. One of `rdf_triples_xml`, `rdf_triples_turtle`, or `rdf_quads_nq`. |
| `--rdf_graph <URI>` | No | Named graph(s) to load when using `rdf_quads_nq`. Can be specified multiple times. If omitted, all graphs are loaded. |
| `--nest_objects_of_predicate <URI>` | No | Predicates whose objects should be inlined (nested) into the subject rather than emitted as top-level subjects. Can be specified multiple times. |
| `--exclude_objects_of_predicate <URI>` | No | Predicates whose objects should be excluded entirely. Can be specified multiple times. |
| `--reif_pointer_predicate <URI>` | No | Predicates that point to a reification metadata object. Can be specified multiple times. |
| `--reif_predicate_predicate <URI>` | No | Predicates from a reification metadata object to the reified predicate. Can be specified multiple times. |
| `--reif_value_predicate <URI>` | No | Predicates from a reification metadata object to the reified value. Can be specified multiple times. |
| `--rdf_types_are_grebi_types` | No | Flag. If set, `rdf:type` values are also emitted as `grebi:type`. Defaults to false. |

## Behaviour

1. The entire RDF graph is loaded into memory.
2. OWL axiom subjects (`owl:Axiom`) and RDF statement subjects (`rdf:Statement`) are identified.
3. Reification metadata is resolved: pointer → predicate → value triples are collapsed into annotated edges.
4. Objects of `--nest_objects_of_predicate` predicates are inlined and excluded from top-level output.
5. Objects of `--exclude_objects_of_predicate` and `--reif_pointer_predicate` predicates are excluded from top-level output.
6. Each remaining subject is written as a JSONL record with its properties.
