
from shared import config

config['id'] = 'TestCliqueMerge'
config['name'] = 'Test: Clique Merge'
config['datasource_configs'] = [
    "./configs/datasource_configs/test_clique_merge.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))

