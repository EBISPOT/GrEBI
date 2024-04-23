
import json
import os
import sys
import shlex
import time
from subprocess import Popen, PIPE, STDOUT

def main():

    if len(sys.argv) < 2:
        print("Usage: grebi_index.slurm.py <grebi_config.json>")
        exit(1)

    config_filename = os.path.abspath(sys.argv[1])
    print(get_time() + " --- Config filename: " + config_filename, flush=True)

    with open(config_filename, 'r') as f:
        config = json.load(f)

    input_merged_gz_filenames = os.path.join(os.environ['GREBI_HPS_TMP'], "03_merge", "merged.jsonl.*")
    out_path = os.path.join(os.environ['GREBI_HPS_TMP'], "04_index", "rocksdb")
    output_metadata_filename = os.path.join(os.environ['GREBI_HPS_TMP'], "04_index", "metadata.json")

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    cmd = 'zcat ' + input_merged_gz_filenames + ' | '
    cmd = cmd + ' ./target/release/grebi_json2rocks '
    cmd = cmd + ' --rocksdb-path ' + out_path
    cmd = cmd + ' > ' + output_metadata_filename

    print(get_time() + " --- Running index command: " + cmd, flush=True)

    exitcode = os.system('bash -c "' + cmd + '"')
    if exitcode != 0:
        print(get_time() + " --- index command failed with exit code " + str(exitcode), flush=True)
        exit(2)

    os.sync()

    print(get_time() + " --- Calculating rocksdb size")
    os.system('du -shl ' + out_path)
    print(get_time() + " --- Calculating rocksdb size complete")



def exists(x):
    res = os.path.exists(x)
    if not res:
        print("Skipping missing result file: " + x, flush=True)
    return res

def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
    main()

    




    






