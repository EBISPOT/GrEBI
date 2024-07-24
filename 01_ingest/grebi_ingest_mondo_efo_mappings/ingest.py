#!/usr/bin/env python3

import sys
import json
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--datasource-name', required=True)
parser.add_argument('--filename', required=False)
args = parser.parse_args()

for line in sys.stdin:
    tokens = line.strip().split('\t')
    print(json.dumps({ 'id': tokens[0], 'grebi:datasource': args.datasource_name, 'grebi:equivalentTo': tokens[1].split(',') }))
