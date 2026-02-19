
from shared import config

config['id'] = 'HRA_KG'
config['name'] = 'Human Reference Atlas KG'
config['datasource_configs'] = [
    "./configs/datasource_configs/hra_kg.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))
