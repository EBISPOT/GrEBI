

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

with open('extra_prefixes.json', 'r') as extra:
    j = json.load(extra)
    for prefix, uri_prefix in j.items():
        if prefix in normalise:
            norm_prefix = normalise[prefix]
        else:
            norm_prefix = prefix
        if uri_prefix in normalise:
            norm_uri_prefix = normalise[uri_prefix]
        else:
            norm_uri_prefix = uri_prefix
        if not uri_prefix in compact:
            compact[uri_prefix] = norm_prefix
        if not uri_prefix in normalise:
            normalise[uri_prefix] = norm_prefix
        if not prefix in expand:
            expand[prefix] = norm_uri_prefix
        if not prefix in normalise:
            normalise[prefix] = norm_prefix

with open('prefix_map_compact.json', 'w') as outfile:
    json.dump(compact, outfile, indent=2)
with open('prefix_map_expand.json', 'w') as outfile:
    json.dump(expand, outfile, indent=2)
with open('prefix_map_normalise.json', 'w') as outfile:
    json.dump(normalise, outfile, indent=2)


