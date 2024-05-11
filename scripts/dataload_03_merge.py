

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
    ### 3. Merge
    ###
    if os.environ['GREBI_USE_SLURM'] == "1":
        print("Merging on slurm (use_slurm = true)")
        slurm_cmd = ' '.join([
            'srun',
            '--time=' + config['slurm_max_time']['merge'],
            '--mem=' + config['slurm_max_memory']['merge'],
            './03_merge/grebi_merge.slurm.sh',
            config_filename,
            datasource_files_listing
        ])
        if os.system(slurm_cmd) != 0:
            print("Failed to merge, command was: " + slurm_cmd)
            exit(1)
    else:
        print("Merging locally (use_slurm = false)")
        cmd = ' '.join([
            './03_merge/grebi_merge.slurm.sh',
            config_filename,
            datasource_files_listing
        ])
        print(cmd)
        if os.system(cmd) != 0:
            print("Failed to merge")
            exit(1)
    os.sync()

def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
   main()





