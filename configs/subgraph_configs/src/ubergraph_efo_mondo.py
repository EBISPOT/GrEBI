

# Pilot subgraph for the owlmake-built ubergraph path: EFO + MONDO sourced from
# OWL (not OLS), preprocessed by `om ubergraph` into one ubergraph.nq, then
# ingested as three datasources — the merged ontology graph (annotations) plus
# the redundant and non-redundant relation graphs as SEPARATE datasources so
# queries can include or exclude the transitive closure.
#
# No OLS and no embeddings here: this proves the OWL-native path standalone. The
# shared equivalence groups already normalise the raw OWL vocabulary
# (rdfs:label, rdfs:subClassOf, iao:definition, oboinowl synonyms), so no
# merge-config changes are needed.

from shared import config

config['id'] = 'UbergraphEfoMondo'
config['name'] = 'Ubergraph pilot (EFO + MONDO, owlmake-built)'

config['ontology_sets'] = [
    "./configs/ontology_configs/efo_mondo.yaml"
]

config['datasource_configs'] = [
    "./configs/datasource_configs/ontologies/ontologies.yaml",
    "./configs/datasource_configs/ontologies/ontologies_nonredundant.yaml",
    "./configs/datasource_configs/ontologies/ontologies_redundant.yaml",
]

# Prefer the merged ontology graph for labels/definitions over the relation graphs.
config['prioritise_datasources'] = [
    "Ontologies",
    "Ontologies.nonredundant",
    "Ontologies.redundant",
] + config['prioritise_datasources']

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))
