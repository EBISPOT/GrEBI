
from shared import config

config['id'] = 'HelloWorld'
config['name'] = 'Hello World example'
config['datasource_configs'] = [
    "./configs/datasource_configs/hello_world.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))


