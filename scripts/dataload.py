
import json
import sys
import os

os.environ['GREBI_HOME'] = os.path.dirname(sys.path[0])
os.chdir(os.environ['GREBI_HOME'])

os.environ['GREBI_CONFIG'] = sys.argv[1]

config = json.load(open(f'./configs/pipeline_configs/${sys.argv[1]}.json'))

for subgraph in config['subgraphs']:
    os.environ['GREBI_SUBGRAPH'] = subgraph
    os.system('nextflow ./nextflow/01_create_subgraph.nf')






