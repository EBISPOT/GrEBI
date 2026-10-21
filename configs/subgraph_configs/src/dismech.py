

from shared import config

config['id'] = 'dismech'
config['name'] = 'Disorder Mechanisms Knowledge Base'
config['datasource_configs'] = [
    "./configs/datasource_configs/ols/ols_mondo.yaml",
    "./configs/datasource_configs/ols/ols_hp.yaml",
    "./configs/datasource_configs/mondo_other_hierarchy.yaml",
    "./configs/datasource_configs/ols/ols_uberon.yaml",
    "./configs/datasource_configs/ols/embeddings__llama-embed-nemotron-8b/ols_mondo_embeddings__llama-embed-nemotron-8b.yaml",
    "./configs/datasource_configs/ols/embeddings__llama-embed-nemotron-8b/ols_hp_embeddings__llama-embed-nemotron-8b.yaml",
    "./configs/datasource_configs/ols/embeddings__llama-embed-nemotron-8b/ols_uberon_embeddings__llama-embed-nemotron-8b.yaml",
    "./configs/datasource_configs/ols/embeddings__llama-embed-nemotron-8b/ols_pca_model__llama-embed-nemotron-8b.yaml",
    "./configs/datasource_configs/dismech.yaml"
]

# MONDO's disease-to-HP-phenotype xrefs (the MONDO.otherHierarchy datasource)
# are identifiers here, so a disease and its phenotype become one node
config['identifier_props'].insert(config['identifier_props'].index('skos:exactMatch') + 1, 'mondo:otherHierarchy')

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))

