from shared import config

config['id'] = 'PRIDE'
config['name'] = 'PRIDE Archive project metadata'
config['datasource_configs'] = [
    "./configs/datasource_configs/pride.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))
