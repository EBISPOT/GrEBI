#!/usr/bin/env python3

import sys
import json
import argparse
import pandas as pd
import io
import re

parser = argparse.ArgumentParser()
parser.add_argument('--datasource-name', required=True)
parser.add_argument('--filename', required=False)
args = parser.parse_args()

df = pd.read_excel(io.BytesIO(sys.stdin.buffer.read()), dtype=str)
df.rename(columns={col: 'grebi:name' for col in df.columns if col == 'PRODUCT_NAME'}, inplace=True)
df['id'] = 'appril:'+df['REG_NUM']
df['grebi:type'] = 'hett:PesticideProduct'
df['grebi:datasource'] = args.datasource_name

df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

for obj in df.to_dict(orient='records'):
    obj = {re.sub(r'[^\w\s:]', '',k): v for k, v in obj.items() if pd.notna(v)}

    if 'PESTS' in obj:
        obj['PESTS'] = list(map(lambda p: p.strip(), obj['PESTS'].split(',')))
    if 'SITES' in obj:
        obj['SITES'] = list(map(lambda p: p.strip(), obj['SITES'].split(',')))

    if 'AIS' in obj:
        cas = list(map(lambda cas: 'cas:'+cas, re.findall(r'\d{1,7}-\d{2}-\d', obj['AIS'])))
        for c in cas:
            print(json.dumps({'id': c, 'grebi:type': 'grebi:Chemical', 'grebi:datasource': args.datasource_name}))
        obj['hett:hasActiveIngredient'] = cas

    if 'INERTS' in obj:
        cas = list(map(lambda cas: 'cas:'+cas, re.findall(r'\d{1,7}-\d{2}-\d', obj['INERTS'])))
        for c in cas:
            print(json.dumps({'id': c, 'grebi:type': 'grebi:Chemical', 'grebi:datasource': args.datasource_name}))
        obj['hett:hasInertIngredient'] = cas

    print(json.dumps(obj))

