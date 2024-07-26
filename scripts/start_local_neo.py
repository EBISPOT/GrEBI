#!/usr/bin/env python3

import json
import os
import sys
import shlex
import time
import glob
from subprocess import Popen, PIPE, STDOUT

GREBI_HOME = os.environ['GREBI_HOME']
GREBI_CONFIG = os.environ['GREBI_CONFIG']
GREBI_TMP = os.environ['GREBI_TMP']

def main():
    neo_data_path = os.path.join( os.environ['GREBI_TMP'], os.environ['GREBI_CONFIG'], "combined_neo4j", "data")

    cmd = ' '.join([
        'docker run',
        '-p 7474:7474',
        '-p 7687:7687',
        '-v ' + shlex.quote(neo_data_path) + ':/data',
        '-e NEO4J_AUTH=none',
        '-e NEO4J_PLUGINS=\\[\\"apoc\\"\\]',
        'neo4j:5.18.0'
    ])

    os.system(cmd)


if __name__=="__main__":
    main()

    
