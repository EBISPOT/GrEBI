
import json
import os
import sys
import shlex
import time
import glob
import argparse
from subprocess import Popen, PIPE, STDOUT

def main():

    parser = argparse.ArgumentParser(description='Materialise Cypher queries as CSV')
    parser.add_argument('--in-db-path', type=str, help='Path with the neo4j database to query', required=True)
    parser.add_argument('--out-jsons-path', type=str, help='Path for the output json files of materialised results', required=True)
    args = parser.parse_args()

    has_singularity = os.system('which singularity') == 0

    print(get_time() + " --- Run queries")

    neo_path = args.in_db_path
    neo_data_path = os.path.abspath(os.path.join(neo_path, "data"))
    neo_logs_path = os.path.abspath(os.path.join(neo_path, "logs"))

    jsons_path = args.out_jsons_path

    os.makedirs(jsons_path)

    if has_singularity:
        cmd = ' '.join([
            'JAVA_OPTS=\'-server -Xms50g -Xmx50g\'',
            'singularity run',
            '--bind ' + os.path.abspath(".") + ':/mnt',
            '--bind ' + shlex.quote(neo_data_path) + ':/data',
            '--bind ' + shlex.quote(neo_logs_path) + ':/logs',
            '--bind ' + os.path.abspath(os.path.join(os.environ['GREBI_DATALOAD_HOME'], '07_run_queries/run_queries.dockerpy')) + ':/run_queries.py',
            '--bind ' + os.path.abspath(os.environ['GREBI_QUERY_YAMLS_PATH']) + ':/materialised_queries',
            '--bind ' + os.path.abspath(args.out_jsons_path) + ':/out',
            '--writable-tmpfs',
            '--network=none',
            '--env NEO4J_AUTH=none',
            '--env NEO4J_server_memory_heap_initial__size=300G',
            '--env NEO4J_server_memory_heap_max__size=300G',
            '--env NEO4J_server_memory_pagecache_size=150G',
            '--env NEO4J_dbms_memory_transaction_total_max=150G',
            '--env TINI_SUBREAPER=true',
            '--env GREBI_SUBGRAPH='+os.environ['GREBI_SUBGRAPH'],   
            'docker://ghcr.io/ebispot/grebi_neo4j_with_extras:2025.03.0',
            'python3 /run_queries.py'
        ])
    else:
        cmd = ' '.join([
            'docker run',
            '--user="$(id -u):$(id -g)"',
            '-v ' + shlex.quote(neo_data_path) + ':/data',
            '-v ' + shlex.quote(neo_logs_path) + ':/logs',
            '-v ' + os.path.abspath(os.path.join(os.environ['GREBI_DATALOAD_HOME'], '07_run_queries/run_queries.dockerpy')) + ':/run_queries.py',
            '-v ' + os.path.abspath(os.environ['GREBI_QUERY_YAMLS_PATH']) + ':/materialised_queries',
            '-v ' + os.path.abspath(args.out_jsons_path) + ':/out',
            '-e NEO4J_AUTH=none',
            '-e GREBI_SUBGRAPH='+os.environ['GREBI_SUBGRAPH'],   
            'ghcr.io/ebispot/grebi_neo4j_with_extras:2025.03.0',
            'python3 /run_queries.py'
        ])

    print(cmd)

    if os.system(cmd) != 0:
        print("neo4j import failed")
        exit(1)


def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
    main()

    
