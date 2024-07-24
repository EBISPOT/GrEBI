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

df = pd.read_excel(io.BytesIO(sys.stdin.buffer.read()), skiprows=2, dtype=str)
# df.rename(columns={col: 'Status' for col in df.columns if col.startswith('Category')}, inplace=True)

df['id'] = df['Substance']
df.rename(columns={col: 'grebi:name' for col in df.columns if col == 'Substance'}, inplace=True)

df['grebi:type'] = 'hett:AgroSubstance'
df['grebi:datasource'] = args.datasource_name

df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

for obj in df.to_dict(orient='records'):
    obj = {k: v for k, v in obj.items() if pd.notna(v)}

    if 'Authorised' in obj:
        obj['Authorised'] = list(map(lambda p: p.strip(), obj['Authorised'].split(',')))

    if 'CAS Number' in obj:
        # match cas numbers by regex
        cas =  list(map(lambda cas: 'cas:'+cas, re.findall(r'\d{1,7}-\d{2}-\d', obj['CAS Number'])))
        for c in cas:
            print(json.dumps({'id': c, 'grebi:type': 'grebi:Chemical', 'grebi:datasource': args.datasource_name}))
        obj['CAS Number'] = cas

    print(json.dumps(obj))

