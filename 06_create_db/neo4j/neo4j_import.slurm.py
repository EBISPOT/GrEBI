
import json
import os
import sys
import shlex
import time
import glob
from subprocess import Popen, PIPE, STDOUT

def main():

    if len(sys.argv) < 2:
        print("Usage: neo4j_import.py <grebi_config.json>")
        exit(1)

    config_filename = os.path.abspath(sys.argv[1])
    print(get_time() + " --- Config filename: " + config_filename, flush=True)

    with open(config_filename, 'r') as f:
        config = json.load(f)

    nodes = glob.glob(os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "05_materialize_edges", "n4nodes_*"))
    edges = glob.glob(os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "05_materialize_edges", "n4edges_*"))
    neo_path = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "06_create_db", "neo4j")
    neo_data_path = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "06_create_db", "neo4j", "data")
    neo_logs_path = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "06_create_db", "neo4j", "logs")

    os.system('rm -rf ' + shlex.quote(neo_path))
    os.makedirs(neo_data_path, exist_ok=True)
    os.makedirs(neo_logs_path, exist_ok=True)

    neo_cmd = [
        'neo4j-admin database import full',
    ] + list(map(lambda f: '--nodes ' + shlex.quote("/mnt/" + os.path.basename(f)), nodes)) + list(map(lambda f: '--relationships ' + shlex.quote("/mnt/" + os.path.basename(f)), edges)) + [
     '--ignore-empty-strings=true',
     '--array-delimiter=";"',
     '--threads=32',
     '--max-off-heap-memory=50G',
     '--verbose',
     '--read-buffer-size=16m'
    ]

    if config['use_slurm'] == True:
        cmd = ' '.join([
            'JAVA_OPTS=\'-server -Xms50g -Xmx50g\'',
            'singularity run',
            '--bind ' + os.path.abspath(os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "05_materialize_edges")) + ':/mnt',
            '--bind ' + shlex.quote(neo_data_path) + ':/data',
            '--bind ' + shlex.quote(neo_logs_path) + ':/logs',
            '--writable-tmpfs',
            'docker://neo4j:5.18.0'
        ] + neo_cmd)
    else:
        cmd = ' '.join([
            'docker run',
            '-v ' + os.path.abspath(os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "05_materialize_edges")) + ':/mnt',
            '-v ' + shlex.quote(neo_data_path) + ':/data',
            '-v ' + shlex.quote(neo_logs_path) + ':/logs',
            'neo4j:5.18.0'
        ] + neo_cmd)

    print(cmd)

    if os.system(cmd) != 0:
        print("neo4j import failed")
        exit(1)


def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
    main()

    
