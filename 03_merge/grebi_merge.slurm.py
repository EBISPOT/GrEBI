
import json
import os
import sys
import shlex
import time
from subprocess import Popen, PIPE, STDOUT

def main():

    if len(sys.argv) < 3:
        print("Usage: grebi_merge.slurm.py <grebi_config.json> <datasource_files.jsonl>")
        exit(1)

    config_filename = os.path.abspath(sys.argv[1])
    datasource_files_filename = os.path.abspath(sys.argv[2])

    print("--- Config filename: " + config_filename, flush=True)
    print("--- Datasources list: " + datasource_files_filename, flush=True)

    with open(config_filename, 'r') as f:
        config = json.load(f)

    with open(datasource_files_filename, 'r') as f:
        datasource_files = f.readlines()

    datasource_files = map(lambda x: json.loads(x.strip()), datasource_files)

    result_filenames = map(
        lambda x: x['artefacts']['sorted_expanded_subjects_jsonl_gz'],
        datasource_files)

    # filter result_filenames for files that exist only
    result_filenames = list(filter(lambda x: exists(x), result_filenames))


    out_path = os.path.join(config['worker_output_dir'], "03_merge", "merged.jsonl.gz")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    cmd = './target/release/grebi_merge ' + ' '.join(result_filenames)
    cmd = cmd + " | pigz --fast > " + shlex.quote(out_path)

    print("Running merge command: " + cmd, flush=True)

    exitcode = os.system('bash -c "' + cmd + '"')
    if exitcode != 0:
        print(get_time() + " --- merge command failed with exit code " + str(exitcode), flush=True)
        exit(2)



def exists(x):
    res = os.path.exists(x)
    if not res:
        print("Skipping missing result file: " + x, flush=True)
    return res

def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
    main()

    




    






