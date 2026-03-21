
from shared import config

config['id'] = 'TestMultiDatasource'
config['name'] = 'Test: Multi Datasource Merge'
config['datasource_configs'] = [
    "./configs/datasource_configs/test/test_multi_datasource_a.yaml",
    "./configs/datasource_configs/test/test_multi_datasource_b.yaml"
]
config['prioritise_datasources'] = [
    "TestMultiDatasourceA"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))

