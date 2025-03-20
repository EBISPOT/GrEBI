#!/usr/bin/env python3

import pandas
import sys
import json

df = pandas.read_csv(sys.stdin, dtype=str)

for row in df.to_dict(orient="records"):

    x_source = row['x_source']
    y_source = row['y_source']

    if x_source == "NCBI":
        x_source = "NCBIGene"
    if y_source == "NCBI":
        y_source = "NCBIGene"

    x_id = x_source + ':' + row['x_id']
    y_id = y_source + ':' + row['y_id']

    res = {
        'id': x_id,
        'grebi:name': row['x_name'],
        'grebi:type': 'biolink:Entity'
    }

    rel = "primekg:" + row['relation']

    res[rel] = {
        'grebi:value': y_id,
        'grebi:properties': {f"primekg:{key}": [value] for key, value in row.items()}
    }

    print(json.dumps(res))

    res2 = {
        'id': y_id,
        'grebi:name': row['y_name'],
        'grebi:type': 'biolink:Entity'
    }
    print(json.dumps(res2))

    
