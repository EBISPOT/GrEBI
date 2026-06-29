# Ontology sets

An *ontology set* is a first-class, OWL-native input to the pipeline: a list of
ontologies (loaded from OWL, not OLS) that the ontology preprocess phase builds
into a single ubergraph with owlmake's `om ubergraph` command. The resulting
`ubergraph.nq` is ingested as the `Ontologies*` datasources
(`configs/datasource_configs/ontologies/`), each reading one of its named
graphs:

- `…/ontology`      — merged, reasoned ontology: labels, synonyms, definitions, axioms
- `…/nonredundant`  — direct relations (transitive reduction)
- `…/redundant`     — full transitive closure

Redundant and non-redundant are loaded as **separate datasources** so queries
can include or exclude the closure. Per-datasource predicate mapping
(`grebi_rdf2jsonl --map-predicate`) emits `subClassOf` as `biolink:subclass_of`
(direct) from the non-redundant graph and `biolink:broad_match` (transitive)
from the redundant graph, matching the previous OLS
`directParent`/`directAncestor` behaviour.

This is the motivating use case for building our own ubergraph rather than using
the RENCI download: it can include non-OBO ontologies such as **EFO**, which the
OBO-only RENCI Ubergraph cannot.

## TODO

- **Materialise sub-properties and inverse properties in the ubergraph build.**
  The relation graph currently materialises existential restrictions and the
  subclass closure, but not (fully) sub-property entailments or inverse
  properties (EL reasoning does not cover inverses). Materialising these in
  `om ubergraph` (owlmake) would complete relation querying — exposing both
  directions of a relation and propagating sub-property edges — handling cases
  analogous to the subclass closure already split across the redundant /
  non-redundant graphs.
