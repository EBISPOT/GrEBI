# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "bioregistry",
# ]
# ///


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
    for left, right in j.items():
        if left in normalise:
            if right in normalise:
                if normalise[left] != normalise[right]:
                    raise ValueError(f'Conflict in extra prefixes for {left} and {right}')
            else:
                normalise[right] = normalise[left]
        elif right in normalise:
            normalise[left] = normalise[right]
        else:
            normalise[left] = right

with open('prefix_map_normalise.json', 'w') as outfile:
    json.dump(normalise, outfile, indent=2)


