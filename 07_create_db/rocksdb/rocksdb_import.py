

import json
import os
import sys
import shlex
import time
import glob
from subprocess import Popen, PIPE, STDOUT

def main():
    config_filename = os.path.abspath(os.path.join('./configs/pipeline_configs/', os.environ['GREBI_CONFIG'] + '.json'))
    print(get_time() + " --- Config filename: " + config_filename, flush=True)

    with open(config_filename, 'r') as f:
        config = json.load(f)

    if config['use_slurm'] == True:
        cmd = ' '.join([
            'srun -t 4:0:0 --mem 400g -c 8',
            './07_create_db/rocksdb/rocksdb_import.slurm.sh',
            config_filename
        ])
    else:
        cmd = ' '.join([
            './07_create_db/rocksdb/rocksdb_import.slurm.sh',
            config_filename
        ])

    if os.system(cmd) != 0:
        print("rocksdb import failed")
        exit(1)

def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
    main()

    
