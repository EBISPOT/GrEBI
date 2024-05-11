

import json
import os
import sys
import shlex
import time
import glob
from subprocess import Popen, PIPE, STDOUT

def main():

    if len(sys.argv) < 2:
        print("Usage: rocksdb_import.py <grebi_config.json>")
        exit(1)

    config_filename = os.path.abspath(sys.argv[1])

    print(get_time() + " --- Create RocksDB")
    print(get_time() + " --- Config filename: " + config_filename, flush=True)

    with open(config_filename, 'r') as f:
        config = json.load(f)

    final_rocksdb_path = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "07_create_db", "rocksdb")

    if config['use_slurm'] == True:
        rocksdb_path="/dev/shm/rocksdb"
    else:
        rocksdb_path=final_rocksdb_path

    os.system('rm -rf ' + shlex.quote(rocksdb_path))
    os.makedirs(rocksdb_path, exist_ok=True)

    files = os.path.abspath(os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "05_materialise/*.jsonl"))

    cmd = ' '.join([
        'cat ' + files + ' |',
        './target/release/grebi_make_rocks',
        '--rocksdb-path ' + shlex.quote(rocksdb_path)
    ])

    print(cmd)

    if os.system(cmd) != 0:
        print("rocksdb import failed")
        exit(1)

    if config['use_slurm'] == True:
        if os.system("mv " + shlex.quote(rocksdb_path) + " " + shlex.quote(final_rocksdb_path)) != 0:
            print("failed moving rocksdb to nfs")
            exit(1)

def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
    main()

    
