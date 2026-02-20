
from shared import config

config['id'] = 'TestTypeHierarchy'
config['name'] = 'Test: Type Hierarchy'
config['datasource_configs'] = [
    "./configs/datasource_configs/test_type_hierarchy.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))

