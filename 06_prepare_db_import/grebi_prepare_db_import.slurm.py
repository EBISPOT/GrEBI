

import json
import os
import sys
import shlex
import time
import glob
from subprocess import Popen, PIPE, STDOUT

def main():
    if len(sys.argv) < 2:
        print("Usage: grebi_prepare_db_import.slurm.py <grebi_config.json>")
        exit(1)

    config_filename = os.path.abspath(sys.argv[1])

    print(get_time() + " --- Config filename: " + config_filename, flush=True)

    task_id = os.getenv('SLURM_ARRAY_TASK_ID')
    if task_id == None:
        print("Slurm task ID not found; is this definitely running as part of the pipeline?\n", flush=True);
        exit(1)

    with open(config_filename, 'r') as f:
        config = json.load(f)

    input_nodes_filenames = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "03_merge", "merged.jsonl.0*")
    input_edges_filenames = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "05_materialise_edges", "edges_*")
    input_summary_json = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "04_index", "summary.json")
    input_metadata_jsonl = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "04_index", "metadata.jsonl")

    final_out_neo_nodes_path = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "06_prepare_db_import", "n4nodes_" + task_id + ".csv")
    final_out_neo_edges_path = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "06_prepare_db_import", "n4edges_" + task_id + ".csv")
    final_out_solr_nodes_path = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "06_prepare_db_import", "solr_nodes_" + task_id + ".jsonl")
    final_out_solr_edges_path = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "06_prepare_db_import", "solr_edges_" + task_id + ".jsonl")

    if config['use_slurm'] == True:
        # Writing directly to the NFS currently sometimes causes large ranges of 0 bytes to be inserted into the files.
        # Temp fix: write to /dev/shm and then mv to the NFS
        out_neo_nodes_path = os.path.join("/dev/shm/n4nodes_" + task_id + ".csv")
        out_neo_edges_path = os.path.join("/dev/shm/n4edges_" + task_id + ".csv")
        out_solr_nodes_path = os.path.join("/dev/shm/solr_nodes_" + task_id + ".jsonl")
        out_solr_edges_path = os.path.join("/dev/shm/solr_edges_" + task_id + ".jsonl")
    else:
        out_neo_nodes_path = final_out_neo_nodes_path
        out_neo_edges_path = final_out_neo_edges_path
        out_solr_nodes_path = final_out_solr_nodes_path
        out_solr_edges_path = final_out_solr_edges_path

    os.makedirs(os.path.dirname(final_out_neo_edges_path), exist_ok=True)

    all_nodes_files = glob.glob(input_nodes_filenames)
    all_edges_files = glob.glob(input_edges_filenames)
    our_nodes_file = list(filter(lambda f: int(f.split('.')[-1]) == int(task_id), all_nodes_files))[0]
    our_edges_file = list(filter(lambda f: int(f.split('/')[-1].split('_')[1].split('.')[0]) == int(task_id), all_edges_files))[0]
    print(get_time() + " --- Our nodes file: " + our_nodes_file)
    print(get_time() + " --- Our edges file: " + our_edges_file)

    cmd = ' '.join([
        './target/release/grebi_make_csv',
        '--in-metadata-jsonl ' + shlex.quote(input_metadata_jsonl),
        '--in-summary-json ' + shlex.quote(input_summary_json),
        '--in-nodes-jsonl ' + shlex.quote(our_nodes_file),
        '--in-edges-jsonl ' + shlex.quote(our_edges_file),
        '--out-nodes-csv-path ' + shlex.quote(out_neo_nodes_path),
        '--out-edges-csv-path ' + shlex.quote(out_neo_edges_path)
    ])

    if os.system('bash -c "' + cmd + '"') != 0:
        print("prepare_db_import neo4j failed")
        exit(1)

    cmd = ' '.join([
        './target/release/grebi_make_solr',
        '--in-metadata-jsonl ' + shlex.quote(input_metadata_jsonl),
        '--in-summary-json ' + shlex.quote(input_summary_json),
        '--in-nodes-jsonl ' + shlex.quote(our_nodes_file),
        '--in-edges-jsonl ' + shlex.quote(our_edges_file),
        '--out-nodes-jsonl-path ' + shlex.quote(out_solr_nodes_path),
        '--out-edges-jsonl-path ' + shlex.quote(out_solr_edges_path)
    ])

    if os.system('bash -c "' + cmd + '"') != 0:
        print("prepare_db_import solr failed")
        exit(1)

    if config['use_slurm'] == True:
        cmd = ' '.join([
            "mv " + shlex.quote(out_solr_nodes_path) + " " + shlex.quote(final_out_solr_nodes_path),
            "&&", "mv " + shlex.quote(out_solr_edges_path) + " " + shlex.quote(final_out_solr_edges_path),
            "&&", "mv " + shlex.quote(out_neo_nodes_path) + " " + shlex.quote(final_out_neo_nodes_path),
            "&&", "mv " + shlex.quote(out_neo_edges_path) + " " + shlex.quote(final_out_neo_edges_path)
        ])
        if os.system('bash -c "' + cmd + '"') != 0:
            print("moving files failed")
            exit(1)


def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
    main()

