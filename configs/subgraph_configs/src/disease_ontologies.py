

from shared import config

config['id'] = 'DiseaseOntologies'
config['name'] = 'Disease Ontologies (EFO, MONDO, DOID)'
config['datasource_configs'] = [
    "./configs/datasource_configs/ols/ols_efo.yaml",
    "./configs/datasource_configs/ols/ols_mondo.yaml",
    "./configs/datasource_configs/ols/ols_doid.yaml",
    "./configs/datasource_configs/embeddings__llama-embed-nemotron-8b.yaml",
    "./configs/datasource_configs/embeddings__text-embedding-3-small.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))

