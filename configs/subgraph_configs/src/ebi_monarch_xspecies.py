
from ebi_monarch import config

config['id'] = 'EBI_MONARCH_XSPECIES'
config['name'] = 'EBI Resources and MONARCH Initiative KG with merged cross-species phenotypes'
config['identifier_props'] = config['identifier_props'] + ['semapv:crossSpeciesExactMatch']

# Add OLS embeddings (llama-embed-nemotron-8b) for all included ontologies
_ols_ontologies = [
    'efo', 'mp', 'hp', 'go', 'ro', 'iao', 'uberon', 'pato', 'oba', 'chebi',
    'bspo', 'obi', 'bfo', 'cob', 'cl', 'so', 'eco', 'pr', 'ncbitaxon', 'oio',
    'biolink', 'mondo', 'doid', 'cheminf', 'dc', 'dcterms', 'ncit', 'edam',
    'upheno', 'ecto',
]
_emb_dir = './configs/datasource_configs/ols/embeddings__llama-embed-nemotron-8b'
config['datasource_configs'] += [
    f'{_emb_dir}/ols_{ont}_embeddings__llama-embed-nemotron-8b.yaml'
    for ont in _ols_ontologies
]
config['datasource_configs'].append(f'{_emb_dir}/ols_pca_model__llama-embed-nemotron-8b.yaml')

_emb_dir2 = './configs/datasource_configs/ols/embeddings__text-embedding-3-small'
config['datasource_configs'] += [
    f'{_emb_dir2}/ols_{ont}_embeddings__text-embedding-3-small.yaml'
    for ont in _ols_ontologies
]
config['datasource_configs'].append(f'{_emb_dir2}/ols_pca_model__text-embedding-3-small.yaml')

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))


