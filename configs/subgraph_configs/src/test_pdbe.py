
from shared import config

config['id'] = 'TestPDBe'
config['name'] = 'Test: PDBe SIFTS structure mappings'
config['datasource_configs'] = [
    "./configs/datasource_configs/test/test_pdbe.yaml",
    "./configs/datasource_configs/test/test_pdbe_refs.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))
