
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

    print(get_time() + " --- Create Neo4j DB")
    print(get_time() + " --- Config filename: " + config_filename, flush=True)

    with open(config_filename, 'r') as f:
        config = json.load(f)

    neo_path = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "07_create_db", "neo4j")
    neo_data_path = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "07_create_db", "neo4j", "data")
    neo_logs_path = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "07_create_db", "neo4j", "logs")

    os.system('rm -rf ' + shlex.quote(neo_path))
    os.makedirs(neo_data_path, exist_ok=True)
    os.makedirs(neo_logs_path, exist_ok=True)

    if config['use_slurm'] == True:
        cmd = ' '.join([
            'JAVA_OPTS=\'-server -Xms50g -Xmx50g\'',
            'singularity run',
            '--bind ' + os.path.abspath(os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "06_prepare_db_import")) + ':/mnt',
            '--bind ' + shlex.quote(neo_data_path) + ':/data',
            '--bind ' + shlex.quote(neo_logs_path) + ':/logs',
            '--bind ' + os.path.abspath('./07_create_db/neo4j/neo4j_import.dockersh') + ':/import.sh',
            '--bind ' + os.path.abspath('./07_create_db/neo4j/create_indexes.cypher') + ':/create_indexes.cypher',
            '--writable-tmpfs',
            '--network=none',
            '--env NEO4J_AUTH=none',
            'docker://neo4j:5.18.0',
            'bash /import.sh'
        ])
    else:
        cmd = ' '.join([
            'docker run',
            '-v ' + os.path.abspath(os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "06_prepare_db_import")) + ':/mnt',
            '-v ' + shlex.quote(neo_data_path) + ':/data',
            '-v ' + shlex.quote(neo_logs_path) + ':/logs',
            '-v ' + os.path.abspath('./07_create_db/neo4j/neo4j_import.dockersh') + ':/import.sh',
            '-v ' + os.path.abspath('./07_create_db/neo4j/create_indexes.cypher') + ':/create_indexes.cypher',
            '-e NEO4J_AUTH=none',
            'neo4j:5.18.0',
            'bash /import.sh'
        ])

    print(cmd)

    if os.system(cmd) != 0:
        print("neo4j import failed")
        exit(1)


def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
    main()

    
