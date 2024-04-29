

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

    if config['use_slurm'] == True:
        print("Running ingest on slurm (use_slurm = true)")
        slurm_cmd = ' '.join([
            'sbatch',
            '--wait',
            '-o ' + os.path.abspath(os.path.join(os.environ['GREBI_NFS_TMP'], os.environ['GREBI_CONFIG'], '01_ingest', 'ingest_%a.log')),
            '--array=0-' + str(len(datasource_files)-1) + '%' + str(config['slurm_max_workers']['ingest']),
            '--time=' + config['slurm_max_time']['ingest'],
            '--mem=' + config['slurm_max_memory']['ingest'],
            './01_ingest/grebi_ingest_worker.slurm.sh',
            datasource_files_listing
        ])
        res = os.system(slurm_cmd)
        os.system("tail -n +1 " + os.path.abspath(os.path.join(os.environ['GREBI_NFS_TMP'], os.environ['GREBI_CONFIG'], '01_ingest', '*.log')))
        if res != 0:
            print("Ingest failed")
            exit(1)
    else:
        print("Running ingest locally (use_slurm = false)")
        for n in range(len(datasource_files)):
            print("Running " + str(n) + " of " + str(len(datasource_files)))
            if os.system('SLURM_ARRAY_TASK_ID=' + str(n) + ' ./01_ingest/grebi_ingest_worker.slurm.sh ' + datasource_files_listing) != 0:
                print("Ingest failed")
                exit(1)
    os.sync()

def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
   main()





