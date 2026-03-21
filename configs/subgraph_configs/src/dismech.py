

from shared import config

config['id'] = 'dismech'
config['name'] = 'Disorder Mechanisms Knowledge Base'
config['datasource_configs'] = [
    "./configs/datasource_configs/ols/ols_mondo.yaml",
    "./configs/datasource_configs/ols/embeddings__llama-embed-nemotron-8b/ols_mondo_embeddings__llama-embed-nemotron-8b.yaml",
    "./configs/datasource_configs/ols/ols_hp.yaml",
    "./configs/datasource_configs/ols/ols_uberon.yaml",
    "./configs/datasource_configs/dismech.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))

