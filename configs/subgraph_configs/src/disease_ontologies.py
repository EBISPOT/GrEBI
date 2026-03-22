

from shared import config

config['id'] = 'DiseaseOntologies'
config['name'] = 'Disease Ontologies (EFO, MONDO, DOID)'
config['datasource_configs'] = [
    "./configs/datasource_configs/ols/ols_efo.yaml",
    "./configs/datasource_configs/ols/ols_mondo.yaml",
    "./configs/datasource_configs/ols/ols_doid.yaml",
    "./configs/datasource_configs/ols/embeddings__llama-embed-nemotron-8b/ols_efo_embeddings__llama-embed-nemotron-8b.yaml",
    "./configs/datasource_configs/ols/embeddings__llama-embed-nemotron-8b/ols_mondo_embeddings__llama-embed-nemotron-8b.yaml",
    "./configs/datasource_configs/ols/embeddings__llama-embed-nemotron-8b/ols_doid_embeddings__llama-embed-nemotron-8b.yaml",
    "./configs/datasource_configs/ols/embeddings__llama-embed-nemotron-8b/ols_pca_model__llama-embed-nemotron-8b.yaml",
    "./configs/datasource_configs/ols/embeddings__text-embedding-3-small/ols_efo_embeddings__text-embedding-3-small.yaml",
    "./configs/datasource_configs/ols/embeddings__text-embedding-3-small/ols_mondo_embeddings__text-embedding-3-small.yaml",
    "./configs/datasource_configs/ols/embeddings__text-embedding-3-small/ols_doid_embeddings__text-embedding-3-small.yaml",
    "./configs/datasource_configs/ols/embeddings__text-embedding-3-small/ols_pca_model__text-embedding-3-small.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))

