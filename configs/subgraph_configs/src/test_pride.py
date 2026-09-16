from shared import config

config['id'] = 'TestPRIDE'
config['name'] = 'Test: PRIDE project metadata'
config['datasource_configs'] = [
    "./configs/datasource_configs/test/test_pride.yaml",
    "./configs/datasource_configs/test/test_pride_refs.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))
