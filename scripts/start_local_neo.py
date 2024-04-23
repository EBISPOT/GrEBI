

import json
import os
import sys
import shlex
import time
import glob
from subprocess import Popen, PIPE, STDOUT

def main():

    if len(sys.argv) < 2:
        print("Usage: start_neo.py <grebi_config.json>")
        exit(1)

    config_filename = os.path.abspath(sys.argv[1])
    print(get_time() + " --- Config filename: " + config_filename, flush=True)

    with open(config_filename, 'r') as f:
        config = json.load(f)

    neo_data_path = os.path.join(os.environ['GREBI_HPS_TMP'], "06_create_db", "neo4j", "data")

    cmd = ' '.join([
        'docker run',
        '-p 7474:7474',
        '-p 7687:7687',
        '-v ' + shlex.quote(neo_data_path) + ':/data',
        '-e NEO4J_AUTH=none',
        'neo4j:5.18.0'
    ])

    os.system(cmd)



def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
    main()

    
