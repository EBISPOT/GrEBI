
import os
import shlex
import time
import json
import sys

config = None

def main():

    global config

    if len(sys.argv) < 3:
        print("Usage: grebi_assign_ids_worker.slurm.py <datasources.jsonl> <equivalences_db>")
        exit(1)

    task_id = os.getenv('SLURM_ARRAY_TASK_ID')

    if task_id == None:
        print("Slurm task ID not found; is this definitely running as part of the pipeline?\n", flush=True);
        exit(1)

    print(get_time() + " --- Datasources list: " + sys.argv[1], flush=True)

    with open(sys.argv[1], 'r') as f:
        datasource_files = f.readlines()

    equivalences_db_path = sys.argv[2]

    datasource_file = json.loads(datasource_files[int(task_id)].strip())

    with open(datasource_file['config'], 'r') as f:
        config = json.load(f)

    datasource_name = datasource_file['datasource']['name']

    print(get_time() + " --- Our task ID: " + task_id, flush=True)
    print(get_time() + " --- Config file: " + datasource_file['config'])
    print(get_time() + " --- Datasource: " + datasource_name, flush=True)

    nodes_jsonl_gz_filename = datasource_file['artefacts']['nodes_jsonl_gz']
    expanded_subjects_jsonl_filename = datasource_file['artefacts']['expanded_subjects_jsonl']
    sorted_expanded_subjects_jsonl_filename = datasource_file['artefacts']['sorted_expanded_subjects_jsonl']
    sorted_expanded_subjects_jsonl_gz_filename = datasource_file['artefacts']['sorted_expanded_subjects_jsonl_gz']

    os.makedirs(os.path.dirname(expanded_subjects_jsonl_filename), exist_ok=True)
    os.makedirs(os.path.dirname(sorted_expanded_subjects_jsonl_filename), exist_ok=True)
    os.makedirs(os.path.dirname(sorted_expanded_subjects_jsonl_gz_filename), exist_ok=True)

    print(get_time() + " --- Loading file: " + nodes_jsonl_gz_filename, flush=True)
    print(get_time() + " --- Writing uncompressed to file: " + expanded_subjects_jsonl_filename, flush=True)
    print(get_time() + " --- Then compressed to file: " + sorted_expanded_subjects_jsonl_gz_filename, flush=True)
    print(get_time() + " --- Using equivalences db: " + equivalences_db_path, flush=True)


    cmd = ' '.join([
        'zcat ' + shlex.quote(nodes_jsonl_gz_filename),
        '| ./target/release/grebi_assign_ids', shlex.quote(equivalences_db_path),
        '>', shlex.quote(expanded_subjects_jsonl_filename)
    ])

    print(get_time() + " --- Running assign IDs script: " + cmd, flush=True)
    exitcode = os.system('bash -c "' + cmd + '"')
    if exitcode != 0:
        print(get_time() + " --- assign IDs script failed with exit code " + str(exitcode), flush=True)
        exit(2)

    # 2. Sort expanded JSONL nodes file
    cmd = 'LC_ALL=C sort -o ' + shlex.quote(sorted_expanded_subjects_jsonl_filename) + ' ' + shlex.quote(expanded_subjects_jsonl_filename)
    print(get_time() + " --- Running sort command: " + cmd, flush=True)
    exitcode = os.system('bash -c "' + cmd + '"')
    if exitcode != 0:
        print(get_time() + " --- sort command failed with exit code " + str(exitcode), flush=True)
        exit(2)

    # 3. Remove unsorted nodes file
    print(get_time() + " --- Removing unsorted file " + expanded_subjects_jsonl_filename, flush=True)
    os.remove(expanded_subjects_jsonl_filename)

    # 4. Gzip sorted nodes file
    cmd = 'pigz --fast -f ' + shlex.quote(sorted_expanded_subjects_jsonl_filename)
    print(get_time() + " --- Running gzip command: " + cmd, flush=True)
    exitcode = os.system('bash -c "' + cmd + '"')
    if exitcode != 0:
        print(get_time() + " --- gzip command failed with exit code " + str(exitcode), flush=True)
        exit(2)

    print(get_time() + " --- done", flush=True);


def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
   main()

