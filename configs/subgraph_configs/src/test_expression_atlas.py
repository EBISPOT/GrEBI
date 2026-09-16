from shared import config

config['id'] = 'TestExpressionAtlas'
config['name'] = 'Test: Expression Atlas gene-anatomy expression'
config['datasource_configs'] = [
    "./configs/datasource_configs/test/test_expression_atlas.yaml",
    "./configs/datasource_configs/test/test_expression_atlas_refs.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))
