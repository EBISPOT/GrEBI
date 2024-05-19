

import json
import os
import sys
import shlex
import time
import glob
from subprocess import Popen, PIPE, STDOUT

def main():

    solr_path = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "07_create_db", "solr", "data")

    cmd = ' '.join([
        'rm -rf ' + shlex.quote(solr_path),
        '&&',
        'mkdir -p ' + shlex.quote(solr_path),
        '&&',
        'cp -f ./07_create_db/solr/solr_config/*.xml ' + solr_path + '/'
        '&&',
        'cp -f ./07_create_db/solr/solr_config/*.cfg ' + solr_path + '/'
    ])

    if os.environ['GREBI_USE_SLURM'] == "1":
        os.system("srun --mem=2g --time=1:0:0 bash -c \"" + cmd + "\"")
    else:
        os.system("bash -c \"" + cmd + "\"")
    
    cmd = ' '.join([
        'parallel --tag --line-buffer :::',
        '"./07_create_db/solr/solr_import.sh grebi_autocomplete 8984"',
        '"./07_create_db/solr/solr_import.sh grebi_nodes 8985"',
        '"./07_create_db/solr/solr_import.sh grebi_edges 8986"'
    ])

    if os.system(cmd) != 0:
        print("solr import failed")
        exit(1)

def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
    main()

    
