from shared import config

config['id'] = 'ExpressionAtlas'
config['name'] = 'Expression Atlas baseline gene expression in anatomy'
config['datasource_configs'] = [
    "./configs/datasource_configs/expression_atlas.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))
