
from shared import config

config['id'] = 'EBI_MONARCH'
config['name'] = 'EBI Resources and MONARCH Initiative KG'
config['datasource_configs'] = [
    "./configs/datasource_configs/gwas.yaml",
    "./configs/datasource_configs/ols_efo.yaml",
    "./configs/datasource_configs/ols_top_k.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))

