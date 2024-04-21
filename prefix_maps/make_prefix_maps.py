

import bioregistry
import json

c = bioregistry.get_converter()

compact = {}
expand = {}
normalise = {}

for prefix, uri_prefix in c.prefix_map.items():
    expand[prefix + ':'] = uri_prefix
    normalise[prefix + ':'] = c.reverse_prefix_map.get(c.prefix_map.get(prefix)) + ":"

for uri_prefix, prefix in c.reverse_prefix_map.items():
    compact[uri_prefix] = prefix + ':'
    normalise[uri_prefix] = prefix + ':'

with open('prefix_map_compact.json', 'w') as outfile:
    json.dump(compact, outfile, indent=2)
with open('prefix_map_expand.json', 'w') as outfile:
    json.dump(expand, outfile, indent=2)
with open('prefix_map_normalise.json', 'w') as outfile:
    json.dump(normalise, outfile, indent=2)


