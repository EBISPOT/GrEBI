
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

    basename = os.path.splitext(os.path.basename(filename))[0]

    nodes_jsonl_gz_filename = os.path.abspath(
        os.path.join(
            os.environ['GREBI_HPS_TMP'],
            os.environ['GREBI_CONFIG'],
            '01_ingest',
            datasource_name,
            basename + '.jsonl.gz' ))

    equivalences_tsv_filename = os.path.abspath(
        os.path.join(
            os.environ['GREBI_HPS_TMP'],
            os.environ['GREBI_CONFIG'],
            '01_ingest',
            datasource_name,
            basename + '.equivalences.tsv' ))

    os.makedirs(os.path.dirname(nodes_jsonl_gz_filename), exist_ok=True)
    os.makedirs(os.path.dirname(equivalences_tsv_filename), exist_ok=True)

    cmd = ''
    if filename.endswith('.xz'):
        cmd = 'xz -d --stdout ' + shlex.quote(filename)
    elif filename.endswith('.gz'):
        cmd = 'gzip -d --to-stdout ' + shlex.quote(filename)
    else:
        cmd = 'cat ' + shlex.quote(filename)
    cmd = cmd + ' | ' + shlex.quote(datasource_file['ingest']['ingest_script'])
    cmd = cmd + ' --datasource-name ' + shlex.quote(datasource_name)
    cmd = cmd + ' --filename ' + shlex.quote( os.path.basename( filename ))
    for param in datasource_file['ingest']['ingest_args']:
        cmd = cmd + ' ' + param['name'] + ' ' + shlex.quote( param['value'] )
    cmd = cmd + ' | ./target/release/grebi_normalise_prefixes '
    cmd = cmd + ' | tee >(./target/release/grebi_extract_equivalences --equivalence-properties ' + ','.join(config['equivalence_props']) + ' > ' + shlex.quote(equivalences_tsv_filename) + ')'
    cmd = cmd + ' | pigz --fast > ' + shlex.quote(nodes_jsonl_gz_filename)

    print(get_time() + " --- Running ingest script: " + cmd, flush=True)
    exitcode = os.system('bash -c "' + cmd + '"')
    if exitcode != 0:
        print(get_time() + " --- ingest script failed with exit code " + str(exitcode), flush=True)
        exit(2)

    print(get_time() + " --- done", flush=True);


def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
   main()

