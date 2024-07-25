
import json
import sys
import os

GREBI_HOME = os.environ['GREBI_HOME']
GREBI_CONFIG = os.environ['GREBI_CONFIG']

config = json.load(open(f'{GREBI_HOME}/configs/pipeline_configs/{GREBI_CONFIG}.json'))

for subgraph in config['subgraphs']:
    print(f"===== LOADING SUBGRAPH: {subgraph} =====")
    os.environ['GREBI_SUBGRAPH'] = subgraph
    ret = os.system(f'nextflow {GREBI_HOME}/nextflow/01_create_subgraph.nf')
    if ret != 0:
        exit(ret)
    print(f"===== FINISHED LOADING SUBGRAPH: {subgraph} =====")

ret = os.system(f'nextflow {GREBI_HOME}/nextflow/02_create_dbs.nf')
if ret != 0:
    exit(ret)

