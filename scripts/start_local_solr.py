

import json
import os
import sys
import shlex
import time
import glob
from subprocess import Popen, PIPE, STDOUT

def main():
    solr_data_path = os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], "06_create_db", "solr")

    cmd = ' '.join([
        'docker run',
        '-p 8983:8983',
        '-v ' + shlex.quote(solr_data_path) + ':/var/solr',
        'solr:9.5.0'
    ])

    os.system(cmd)


def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
    main()

    
