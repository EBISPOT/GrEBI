
from shared import config

config['id'] = 'TestEdgeLinking'
config['name'] = 'Test: Edge Linking'
config['datasource_configs'] = [
    "./configs/datasource_configs/test/test_edge_linking.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))

