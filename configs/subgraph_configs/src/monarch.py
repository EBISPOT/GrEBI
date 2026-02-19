
from shared import config

config['id'] = 'MONARCH'
config['name'] = 'Monarch Initiative KG'

config['datasource_configs'] = [
        "./configs/datasource_configs/monarch.yaml"
]
    
if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))

