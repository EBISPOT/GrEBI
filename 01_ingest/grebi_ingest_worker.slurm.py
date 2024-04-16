
import os
import shlex
import time
import json
import sys

config = None

def main():

    global config

    task_id = os.getenv('SLURM_ARRAY_TASK_ID')

    if task_id == None:
        print("Slurm task ID not found; is this definitely running as part of the pipeline?\n", flush=True);
        exit(1)

    print(get_time() + " --- Datasources list: " + sys.argv[1], flush=True)

    with open(sys.argv[1], 'r') as f:
        datasource_files = f.readlines()

    datasource_file = json.loads(datasource_files[int(task_id)].strip())

    filename = datasource_file['filename']

    with open(datasource_file['config'], 'r') as f:
        config = json.load(f)

    datasource_name = datasource_file['datasource']['name']

    print(get_time() + " --- Our task ID: " + task_id, flush=True)
    print(get_time() + " --- Config file: " + datasource_file['config'])
    print(get_time() + " --- Datasource: " + datasource_name, flush=True)
    print(get_time() + " --- Loading file: " + filename, flush=True)

    nodes_jsonl_filename = datasource_file['artefacts']['nodes_jsonl']
    sorted_nodes_jsonl_filename = datasource_file['artefacts']['sorted_nodes_jsonl']
    sorted_nodes_jsonl_gz_filename = datasource_file['artefacts']['sorted_nodes_jsonl_gz']
    equivalences_tsv_filename = datasource_file['artefacts']['equivalences_tsv']

    os.makedirs(os.path.dirname(nodes_jsonl_filename), exist_ok=True)
    os.makedirs(os.path.dirname(sorted_nodes_jsonl_filename), exist_ok=True)
    os.makedirs(os.path.dirname(sorted_nodes_jsonl_gz_filename), exist_ok=True)
    os.makedirs(os.path.dirname(equivalences_tsv_filename), exist_ok=True)


    # 1. Import from datasource to JSONL

    cmd = ''
    if filename.endswith('.xz'):
        cmd = 'xz -d --stdout ' + shlex.quote(filename)
    elif filename.endswith('.gz'):
        cmd = 'gzip -d --to-stdout ' + shlex.quote(filename)
    else:
        cmd = 'cat ' + shlex.quote(filename)
    cmd = cmd + ' | ' + shlex.quote(datasource_file['ingest']['ingest_script'])
    cmd = cmd + ' --datasource-name ' + shlex.quote( os.path.basename( datasource_file['datasource']['name'] ))
    cmd = cmd + ' --filename ' + shlex.quote( os.path.basename( filename ))
    for param in datasource_file['ingest']['ingest_args']:
        cmd = cmd + ' ' + param['name'] + ' ' + shlex.quote( param['value'] )
    cmd = cmd + ' > ' + shlex.quote(  nodes_jsonl_filename )

    print(get_time() + " --- Running ingest script: " + cmd, flush=True)
    exitcode = os.system('bash -c "' + cmd + '"')
    if exitcode != 0:
        print(get_time() + " --- ingest script failed with exit code " + str(exitcode), flush=True)
        exit(2)

    # 2. Extract equivalences from nodes file
    cmd = 'cat ' + shlex.quote(nodes_jsonl_filename) + ' | '
    cmd = cmd + ' ./target/release/grebi_extract_equivalences --equivalence-properties ' + ','.join(config['equivalence_props']) + ' > ' + shlex.quote(equivalences_tsv_filename)
    print(get_time() + " --- Running extract equivalences command: " + cmd, flush=True)
    exitcode = os.system('bash -c "' + cmd + '"')
    if exitcode != 0:
        print(get_time() + " --- extract equivalences command failed with exit code " + str(exitcode), flush=True)
        exit(2)

    # 3. Sort nodes file
    cmd = 'LC_ALL=C sort -o ' + shlex.quote(sorted_nodes_jsonl_filename) + ' ' + shlex.quote(nodes_jsonl_filename)
    print(get_time() + " --- Running sort command: " + cmd, flush=True)
    exitcode = os.system('bash -c "' + cmd + '"')
    if exitcode != 0:
        print(get_time() + " --- sort command failed with exit code " + str(exitcode), flush=True)
        exit(2)

    # 4. Remove unsorted nodes file
    print(get_time() + " --- Removing unsorted file " + nodes_jsonl_filename, flush=True)
    os.remove(nodes_jsonl_filename)

    # 5. Gzip sorted nodes file
    cmd = 'pigz --fast -f ' + shlex.quote(sorted_nodes_jsonl_filename)
    print(get_time() + " --- Running gzip command: " + cmd, flush=True)
    exitcode = os.system('bash -c "' + cmd + '"')
    if exitcode != 0:
        print(get_time() + " --- gzip command failed with exit code " + str(exitcode), flush=True)
        exit(2)

    print(get_time() + " --- done", flush=True);


def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

def create_rdf2json_command(filename, jsonl_filename):

    global config
    

    cmd = cmd + ' && rm -f ' + shlex.quote(jsonl_filename)

    return cmd


if __name__=="__main__":
   main()

