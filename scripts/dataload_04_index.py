

import json
import os
import glob
import sys
import subprocess
import shlex
import re
import time

def main():

    config_filename = os.path.abspath(os.path.join('./configs/pipeline_configs/', os.environ['GREBI_CONFIG'] + '.json'))

    with open(config_filename, 'r') as f:
        config = json.load(f)

    datasource_files_listing = os.path.abspath( os.path.join(os.environ['GREBI_NFS_TMP'], os.environ['GREBI_CONFIG'], '01_ingest', 'datasource_files.jsonl') )
    datasource_files = open(datasource_files_listing, 'r').read().split('\n')

    ###
    ### 4. Index
    ###
    if config['use_slurm'] == True:
        print("Indexing on slurm (use_slurm = true)")
        slurm_cmd = ' '.join([
            'srun',
            '--time=' + config['slurm_max_time']['index'],
            '--mem=' + config['slurm_max_memory']['index'],
            './04_index/grebi_index.slurm.sh',
            config_filename
        ])
        if os.system(slurm_cmd) != 0:
            print("Failed to index")
            exit(1)
    else:
        print("Indexing locally (use_slurm = false)")
        cmd = ' '.join([
            './04_index/grebi_index.slurm.sh',
            config_filename
        ])
        print(cmd)
        if os.system(cmd) != 0:
            print("Failed to index")
            exit(1)
    os.sync()

def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
   main()





