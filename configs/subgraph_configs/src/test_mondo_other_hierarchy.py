
# E2E test subgraph for the MONDO.otherHierarchy ingest: MONDO's annotated
# disease-to-phenotype xrefs pulled out of a tiny MONDO with owlmake's SPARQL
# and clique-merged as identifiers, with stand-in terms so the expected output
# shows which disease and phenotype became one node and which stayed apart
# (see tests/data/test_mondo_other_hierarchy/README.md).

from shared import config

config['id'] = 'TestMondoOtherHierarchy'
config['name'] = 'Test: MONDO otherHierarchy disease-phenotype merging'
config['datasource_configs'] = [
    "./configs/datasource_configs/test/test_mondo_other_hierarchy.yaml",
    "./configs/datasource_configs/test/test_mondo_other_hierarchy_refs.yaml"
]

# MONDO's disease-to-HP-phenotype xrefs (the MONDO.otherHierarchy datasource)
# are identifiers here, so a disease and its phenotype become one node
config['identifier_props'].insert(config['identifier_props'].index('skos:exactMatch') + 1, 'mondo:otherHierarchy')

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))
