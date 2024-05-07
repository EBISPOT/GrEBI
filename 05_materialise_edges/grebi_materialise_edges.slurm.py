

import json
import os
import sys
import shlex
import time
import glob
from subprocess import Popen, PIPE, STDOUT

def main():
    if len(sys.argv) < 2:
        print("Usage: grebi_materialise_edges.slurm.py <grebi_config.json>")
        exit(1)

    config_filename = os.path.abspath(sys.argv[1])

    print(get_time() + " --- Config filename: " + config_filename, flush=True)

    task_id = os.getenv('SLURM_ARRAY_TASK_ID')
    if task_id == None:
        print("Slurm task ID not found; is this definitely running as part of the pipeline?\n", flush=True);
        exit(1)

    with open(config_filename, 'r') as f:
        config = json.load(f)

    input_merged_gz_filenames = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "03_merge", "merged.jsonl.0*")
    input_summary_json = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "04_index", "summary.json")
    input_metadata_jsonl = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "04_index", "metadata.jsonl")
    out_edges_path = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "05_materialise_edges", "edges_" + task_id + ".jsonl")

    os.makedirs(os.path.dirname(out_edges_path), exist_ok=True)

    all_files = glob.glob(input_merged_gz_filenames)
    our_file = list(filter(lambda f: int(f.split('.')[-1]) == int(task_id), all_files))[0]
    print(get_time() + " --- Our file: " + our_file)

    cmd = ' '.join([
        'cat ' + shlex.quote(our_file) + ' |',
        './target/release/grebi_materialise_edges',
        '--in-metadata-jsonl ' + shlex.quote(input_metadata_jsonl),
        '--in-summary-json ' + shlex.quote(input_summary_json),
        '--exclude ' + ','.join(config['exclude_edges']+config['equivalence_props']),
        '> ' + shlex.quote(out_edges_path)
    ])

    if os.system('bash -c "' + cmd + '"') != 0:
        print("materialise edges failed")
        exit(1)

def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
    main()

