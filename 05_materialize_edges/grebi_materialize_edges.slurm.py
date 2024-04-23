


import json
import os
import sys
import shlex
import time
import glob
from subprocess import Popen, PIPE, STDOUT

def main():
    if len(sys.argv) < 2:
        print("Usage: grebi_materialize_edges.slurm.py <grebi_config.json>")
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
    input_metadata_filename = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "04_index", "metadata.json")
    input_subjects_txt = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "04_index", "subjects.txt")
    # out_nodes_path = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "05_materialize_edges", "n4nodes_" + task_id + ".csv.gz")
    # out_edges_path = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "05_materialize_edges", "n4edges_" + task_id + ".csv.gz")
    out_nodes_path = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "05_materialize_edges", "n4nodes_" + task_id + ".csv")
    out_edges_path = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "05_materialize_edges", "n4edges_" + task_id + ".csv")

    os.makedirs(os.path.dirname(out_edges_path), exist_ok=True)

    all_files = glob.glob(input_merged_gz_filenames)
    our_file = list(filter(lambda f: int(f.split('.')[-2]) == int(task_id), all_files))[0]
    print(get_time() + " --- Our file: " + our_file)

    cmd = ' '.join([
        'zcat ' + shlex.quote(our_file) + ' |',
        './target/release/grebi_make_csv',
        '--in-subjects-txt ' + shlex.quote(input_subjects_txt),
        '--in-metadata-json-path ' + shlex.quote(input_metadata_filename),
        '--out-nodes-csv-path ' + shlex.quote(out_nodes_path),
        '--out-edges-csv-path ' + shlex.quote(out_edges_path),
        '--exclude ' + ','.join(config['exclude_edges']+config['equivalence_props'])
    ])

    if os.system('bash -c "' + cmd + '"') != 0:
        print("materialize_edges failed")
        exit(1)

def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
    main()
