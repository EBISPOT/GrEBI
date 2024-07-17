#!/usr/bin/env python3

import sys
import json
import argparse
import pandas as pd
import io

parser = argparse.ArgumentParser()
parser.add_argument('--datasource-name', required=True)
parser.add_argument('--filename', required=False)
args = parser.parse_args()

df = pd.read_excel(io.BytesIO(sys.stdin.buffer.read()), skiprows=3)
df['id'] = df['Substance Name']
df['grebi:datasource'] = args.datasource_name

json_lines = df.to_json(orient='records', lines=True)
print(json_lines)

