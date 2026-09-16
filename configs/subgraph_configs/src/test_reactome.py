
from shared import config

config['id'] = 'TestReactome'
config['name'] = 'Test: Reactome identifier merging'
config['datasource_configs'] = [
    "./configs/datasource_configs/test/test_reactome.yaml",
    "./configs/datasource_configs/test/test_reactome_refs.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))
