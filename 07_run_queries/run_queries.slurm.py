
import json
import os
import sys
import shlex
import time
import glob
from subprocess import Popen, PIPE, STDOUT

def main():

    if len(sys.argv) < 2:
        print("Usage: run_queries.py <grebi_config.json>")
        exit(1)

    config_filename = os.path.abspath(sys.argv[1])
    print(get_time() + " --- Config filename: " + config_filename, flush=True)

    with open(config_filename, 'r') as f:
        config = json.load(f)

    neo_path = os.path.abspath(os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "06_create_db", "neo4j"))
    neo_data_path = os.path.abspath(os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "06_create_db", "neo4j", "data"))
    neo_logs_path = os.path.abspath(os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "06_create_db", "neo4j", "logs"))
    out_path = os.path.abspath(os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "07_run_queries"))

    os.system('rm -rf ' + shlex.quote(out_path))
    os.makedirs(out_path, exist_ok=True)

    if config['use_slurm'] == True:
        cmd = ' '.join([
            'JAVA_OPTS=\'-server -Xms150g -Xmx150g\'',
            'singularity run',
            '--bind ' + os.path.abspath("./queries") + ':/mnt',
            '--bind ' + shlex.quote(out_path) + ':/out',
            '--bind ' + shlex.quote(neo_data_path) + ':/data',
            '--bind ' + shlex.quote(neo_logs_path) + ':/logs',
            '--bind ' + os.path.abspath('./07_run_queries/run_queries.dockerpy') + ':/run_queries.py',
            '--writable-tmpfs',
            '--network=none',
            '--env NEO4J_AUTH=none',
            'docker://ghcr.io/ebispot/grebi_neo4j_with_python:latest',
            'python3 /run_queries.py'
        ])
    else:
        os.system('chmod 777 ' + shlex.quote(out_path))
        cmd = ' '.join([
            'docker run',
            '-v ' + os.path.abspath("./queries") + ':/mnt',
            '-v ' + shlex.quote(out_path) + ':/out',
            '-v ' + shlex.quote(neo_data_path) + ':/data',
            '-v ' + shlex.quote(neo_logs_path) + ':/logs',
            '-v ' + os.path.abspath('./07_run_queries/run_queries.dockerpy') + ':/run_queries.py',
            '-e NEO4J_AUTH=none',
            'ghcr.io/ebispot/grebi_neo4j_with_python:latest',
            'python3 /run_queries.py'
        ])

    print(cmd)

    if os.system(cmd) != 0:
        print("run queries failed")
        exit(1)


def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
    main()

    
