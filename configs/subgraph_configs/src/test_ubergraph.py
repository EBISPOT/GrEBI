

# E2E test subgraph for the owlmake-ubergraph path: builds a tiny synthetic
# ontology with `om ubergraph` and ingests its named graphs as the three
# Ontologies* datasources, exercising the redundant/non-redundant split and the
# subClassOf -> biolink:subclass_of (direct) / biolink:broad_match (transitive)
# mapping end-to-end through the pipeline.

from shared import config

config['id'] = 'TestUbergraph'
config['name'] = 'Test: Ubergraph (redundant / non-redundant)'

config['ontology_sets'] = [
    "./configs/ontology_configs/test_ubergraph.yaml"
]

config['datasource_configs'] = [
    "./configs/datasource_configs/ontologies/ontologies.yaml",
    "./configs/datasource_configs/ontologies/ontologies_nonredundant.yaml",
    "./configs/datasource_configs/ontologies/ontologies_redundant.yaml",
]

config['prioritise_datasources'] = [
    "Ontologies",
    "Ontologies.nonredundant",
    "Ontologies.redundant",
] + config['prioritise_datasources']

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))
