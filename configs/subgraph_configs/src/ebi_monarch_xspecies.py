
from ebi_monarch import config

config['id'] = 'EBI_MONARCH_XSPECIES'
config['name'] = 'EBI Resources and MONARCH Initiative KG with merged cross-species phenotypes'
config['identifier_props'] = config['identifier_props'] + ['semapv:crossSpeciesExactMatch']

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))


