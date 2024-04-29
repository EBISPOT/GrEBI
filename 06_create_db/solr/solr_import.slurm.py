
import json
import os
import sys
import shlex
import time
import glob
from subprocess import Popen, PIPE, STDOUT

def main():

    if len(sys.argv) < 2:
        print("Usage: solr_import.py <grebi_config.json>")
        exit(1)

    config_filename = os.path.abspath(sys.argv[1])
    print(get_time() + " --- Config filename: " + config_filename, flush=True)

    with open(config_filename, 'r') as f:
        config = json.load(f)

    solr_path = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "06_create_db", "solr")

    os.system('rm -rf ' + shlex.quote(solr_path))
    os.makedirs(solr_path, exist_ok=True)
    os.system('chmod 777 ' + solr_path)

    if config['use_slurm'] == True:
        cmd = ' '.join([
            'singularity run',
            '--bind ' + os.path.abspath(os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "05_prepare_db_import")) + ':/data',
            '--bind ' + os.path.abspath('./06_create_db/solr/solr_config') + ':/config',
            '--bind ' + shlex.quote(solr_path) + ':/var/solr',
            '--bind ' + os.path.abspath('./06_create_db/solr/solr_import.dockersh') + ':/import.sh',
            '--writable-tmpfs',
            '--network=none',
            'docker://solr:9.5.0',
            'bash /import.sh'
        ])
    else:
        cmd = ' '.join([
            'docker run',
            '-v ' + os.path.abspath(os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "03_prepare_db_import")) + ':/data',
            '-v ' + os.path.abspath('./06_create_db/solr/solr_config') + ':/config',
            '-v ' + shlex.quote(solr_path) + ':/var/solr',
            '-v ' + os.path.abspath('./06_create_db/solr/solr_import.dockersh') + ':/import.sh',
            'solr:9.5.0',
            'bash /import.sh'
        ])

    print(cmd)

    if os.system(cmd) != 0:
        print("solr import failed")
        exit(1)


def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
    main()

    
