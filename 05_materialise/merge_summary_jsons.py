import sys
import json
from collections import defaultdict

def merge(dict1, dict2):
    for key, value in dict2.items():
        if key in dict1:
            if isinstance(dict1[key], dict) and isinstance(value, dict):
                merge(dict1[key], value)
            else:
                dict1[key] += value
        else:
            dict1[key] = value
    return dict1

merged_data = defaultdict(dict)
for filename in sys.argv[1:]:
    with open(filename, 'r') as file:
        data = json.load(file)
        merge(merged_data, data)

print(json.dumps(merged_data, indent=2))

