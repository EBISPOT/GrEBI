
import json
import os
import sys
import shlex
import time
import glob
import argparse
from subprocess import Popen, PIPE, STDOUT

def main():

    parser = argparse.ArgumentParser(description='Create Neo4j DB')
    parser.add_argument('--in-csv-path', type=str, help='Path with the neo4j import csv files', required=True)
    parser.add_argument('--out-db-path', type=str, help='Path for the new Neo4j database', required=True)
    args = parser.parse_args()

    has_singularity = os.system('which singularity') == 0

    print(get_time() + " --- Create Neo4j DB")

    neo_path = args.out_db_path
    neo_data_path = os.path.abspath(os.path.join(neo_path, "data"))
    neo_logs_path = os.path.abspath(os.path.join(neo_path, "logs"))

    os.makedirs(neo_data_path)
    os.makedirs(neo_logs_path)

    if has_singularity:
        cmd = ' '.join([
            'JAVA_OPTS=\'-server -Xms50g -Xmx50g\'',
            'singularity run'
        ] + list(map(lambda f: "--bind " + os.path.abspath(f) + ":/mnt/" + os.path.basename(f), glob.glob(args.in_csv_path + "/neo_*"))) + [
            '--bind ' + os.path.abspath(args.in_csv_path) + ':/mnt',
            '--bind ' + shlex.quote(neo_data_path) + ':/data',
            '--bind ' + shlex.quote(neo_logs_path) + ':/logs',
            '--bind ' + os.path.abspath(os.path.join(os.environ['GREBI_HOME'], '07_create_db/neo4j/neo4j_import.dockersh')) + ':/import.sh',
            '--bind ' + os.path.abspath(os.path.join(os.environ['GREBI_HOME'], '07_create_db/neo4j/create_indexes.cypher')) + ':/create_indexes.cypher',
            '--writable-tmpfs',
            '--network=none',
            '--env NEO4J_AUTH=none',
            'docker://neo4j:5.18.0',
            'bash /import.sh'
        ])
    else:
        cmd = ' '.join([
            'docker run',
            '--user="$(id -u):$(id -g)"'
        ] + list(map(lambda f: "-v " + os.path.abspath(f) + ":/mnt/" + os.path.basename(f), glob.glob(args.in_csv_path + "/neo_*"))) + [
            '-v ' + os.path.abspath(args.in_csv_path) + ':/mnt',
            '-v ' + shlex.quote(neo_data_path) + ':/data',
            '-v ' + shlex.quote(neo_logs_path) + ':/logs',
            '-v ' + os.path.abspath(os.path.join(os.environ['GREBI_HOME'], '07_create_db/neo4j/neo4j_import.dockersh')) + ':/import.sh',
            '-v ' + os.path.abspath(os.path.join(os.environ['GREBI_HOME'], '07_create_db/neo4j/create_indexes.cypher')) + ':/create_indexes.cypher',
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

    
