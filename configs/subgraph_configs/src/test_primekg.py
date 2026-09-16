
from shared import config

config['id'] = 'TestPrimeKG'
config['name'] = 'Test: PrimeKG identifier padding'
config['datasource_configs'] = [
    "./configs/datasource_configs/test/test_primekg.yaml",
    "./configs/datasource_configs/test/test_primekg_refs.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))
