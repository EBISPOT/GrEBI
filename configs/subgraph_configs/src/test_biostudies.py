from shared import config

config['id'] = 'TestBioStudies'
config['name'] = 'Test: BioStudies metadata'
config['datasource_configs'] = [
    "./configs/datasource_configs/test/test_biostudies.yaml",
    "./configs/datasource_configs/test/test_biostudies_refs.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))
