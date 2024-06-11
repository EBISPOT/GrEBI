#!/usr/bin/env python3

import sys
import json

for line in sys.stdin:
    tokens = line.strip().split('\t')
    print(json.dumps({ 'id': tokens[0], 'grebi:equivalentTo': tokens[1].split(',') }))
