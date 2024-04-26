

import json
import os
import sys
import shlex
import time
import glob
from subprocess import Popen, PIPE, STDOUT

def main():
    neo_data_path = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "06_create_db", "neo4j", "data")

    cmd = ' '.join([
        'docker run',
        '-p 7474:7474',
        '-p 7687:7687',
        '-v ' + shlex.quote(neo_data_path) + ':/data',
        '-e NEO4J_AUTH=none',
        '-e NEO4JLABS_PLUGINS=\\[\\"apoc\\"\\]',
        'neo4j:5.18.0'
    ])

    os.system(cmd)


def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
    main()

    
