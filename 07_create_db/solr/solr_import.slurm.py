
import json
import os
import sys
import shlex
import time
import glob
from subprocess import Popen, PIPE, STDOUT

def main():

    if len(sys.argv) < 3:
        print("Usage: solr_import.py <solr_core> <port>")
        exit(1)

    core = sys.argv[1]
    port = sys.argv[2]

    print(get_time() + " --- Create Solr core")

    solr_core_path = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "07_create_db", "solr", "data", core)

    os.system('rm -rf ' + shlex.quote(solr_core_path))
    os.makedirs(solr_core_path, exist_ok=True)
    os.system('chmod 777 ' + solr_core_path)

    if os.environ['GREBI_USE_SLURM'] == "1":
        cmd = ' '.join([
            'singularity run',
            '--env PYTHONUNBUFFERED=TRUE',
            '--bind ' + os.path.abspath(os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "06_prepare_db_import")) + ':/data',
            '--bind ' + os.path.abspath(os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "04_index", "names.txt")) + ':/names.txt',
            '--bind ' + os.path.abspath('./07_create_db/solr/solr_config') + ':/config',
            '--bind ' + shlex.quote(solr_core_path) + ':/var/solr/data/' + core,
            '--bind ' + os.path.abspath('./07_create_db/solr/solr_import.dockerpy') + ':/import.py',
            '--writable-tmpfs',
            '--net --network=none',
            'docker://ghcr.io/ebispot/grebi_solr_with_python:9.5.0',
            'python3 /import.py', core, port
        ])
    else:
        os.system("chmod 777 " + shlex.quote(solr_core_path))
        cmd = ' '.join([
            'docker run',
            '-e PYTHONUNBUFFERED=TRUE',
            '-v ' + os.path.abspath(os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "06_prepare_db_import")) + ':/data',
            '-v ' + os.path.abspath(os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "04_index", "names.txt")) + ':/names.txt',
            '-v ' + os.path.abspath('./07_create_db/solr/solr_config') + ':/config',
            '-v ' + shlex.quote(solr_core_path) + ':/var/solr/data/' + core,
            '-v ' + os.path.abspath('./07_create_db/solr/solr_import.dockerpy') + ':/import.py',
            'ghcr.io/ebispot/grebi_solr_with_python:9.5.0',
            'python3 /import.py', core, port
        ])

    print(cmd)

    if os.system(cmd) != 0:
        print("solr import failed")
        exit(1)


def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
    main()

    
