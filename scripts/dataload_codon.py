
import json
import sys
import os
import subprocess

GREBI_HOME = os.environ['GREBI_HOME']
GREBI_CONFIG = os.environ['GREBI_CONFIG']

config = json.load(open(f'{GREBI_HOME}/configs/pipeline_configs/{GREBI_CONFIG}.json'))

for subgraph in config['subgraphs']:
    print(f"===== LOADING SUBGRAPH: {subgraph} =====")
    os.environ['GREBI_SUBGRAPH'] = subgraph
    res = os.system(f'NXF_WORK=work_{subgraph} nextflow {GREBI_HOME}/nextflow/01_create_subgraph.nf -c {GREBI_HOME}/nextflow/codon_nextflow.config')
    if res != 0:
        exit(res)
    print(f"===== FINISHED LOADING SUBGRAPH: {subgraph} =====")

res = os.system(f'NXF_WORK=work_combined nextflow {GREBI_HOME}/nextflow/02_create_dbs.nf -c {GREBI_HOME}/nextflow/codon_nextflow.config')
if res != 0:
    exit(res)

