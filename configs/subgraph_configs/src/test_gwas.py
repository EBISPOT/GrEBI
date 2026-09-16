
from shared import config

config['id'] = 'TestGWAS'
config['name'] = 'Test: GWAS Catalog multi-valued columns'
config['datasource_configs'] = [
    "./configs/datasource_configs/test/test_gwas.yaml",
    "./configs/datasource_configs/test/test_gwas_refs.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))
