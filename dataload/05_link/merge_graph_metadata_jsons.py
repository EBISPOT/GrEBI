import numbers
import sys
import json
import os
import glob
from collections import defaultdict

def merge(dict1, dict2, path=""):
    for key, value in dict2.items():
        current_path = f"{path}.{key}" if path else key
        if key in dict1:
            if current_path.startswith("embedding_models2dims.") and isinstance(dict1[key], numbers.Number) and isinstance(value, numbers.Number):
                assert dict1[key] == value, f"embedding_models2dims mismatch for {key}: {dict1[key]} != {value}"
            elif isinstance(dict1[key], dict) and isinstance(value, dict):
                merge(dict1[key], value, current_path)
            elif isinstance(dict1[key], list) and isinstance(value, list):
                for val in value:
                    if val not in dict1[key]:
                        dict1[key].append(val)
            elif isinstance(dict1[key], numbers.Number) and isinstance(value, numbers.Number):
                dict1[key] += value
            elif dict1[key] != value:
                dict1[key] = [dict1[key], value]
        else:
            dict1[key] = value
    return dict1

# Parse arguments: positional args are metadata JSONs, --downloads-dir is optional
metadata_files = []
downloads_dir = None
i = 1
while i < len(sys.argv):
    if sys.argv[i] == '--downloads-dir':
        i += 1
        downloads_dir = sys.argv[i]
    else:
        metadata_files.append(sys.argv[i])
    i += 1

merged_data = defaultdict(dict)
for filename in metadata_files:
    with open(filename, 'r') as file:
        data = json.load(file)
        merge(merged_data, data)

# Inject PCA model JSONs inline under "embedding_pca_models" key
# Searches recursively for any *pca*.json files under the downloads directory
if downloads_dir and os.path.isdir(downloads_dir):
    pca_models = {}
    for fpath in sorted(glob.glob(os.path.join(downloads_dir, '**/*pca*.json'), recursive=True)):
        model_name = os.path.basename(fpath)[:-5]  # strip .json
        with open(fpath, 'r') as f:
            pca_models[model_name] = json.load(f)
        print(f"Loaded PCA model: {model_name} from {fpath}", file=sys.stderr)
    if pca_models:
        merged_data['embedding_pca_models'] = pca_models

print(json.dumps(merged_data, indent=2))

