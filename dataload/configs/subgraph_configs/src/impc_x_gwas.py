

from shared import config

config['id'] = 'IMPC x GWAS'
config['name'] = 'IMPC and the GWAS Catalog'
config['datasource_configs'] = [
    "./configs/datasource_configs/impc.yaml",
    "./configs/datasource_configs/gwas.yaml",
    "./configs/datasource_configs/ols.yaml",
    "./configs/datasource_configs/mondo_efo.yaml",
    "./configs/datasource_configs/monarch.yaml",
    "./configs/datasource_configs/sssom.yaml",
    "./configs/datasource_configs/otar.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))
