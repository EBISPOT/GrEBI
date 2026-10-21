
# E2E test subgraph for the AI_anno datasource: ai_anno's annotation tables
# discovered from a latest.txt manifest and ingested as provenance-carrying
# edges from BioStudies, PRIDE, MetaboLights and GWAS Catalog records (and the
# publications GWAS studies cite) to ontology terms, with stand-in records and
# terms so the expected output shows the edges (see
# tests/data/test_ai_anno/README.md).

from shared import config

config['id'] = 'TestAIAnno'
config['name'] = 'Test: ai_anno annotations of study metadata'
config['datasource_configs'] = [
    "./configs/datasource_configs/test/test_ai_anno.yaml",
    "./configs/datasource_configs/test/test_ai_anno_refs.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))
