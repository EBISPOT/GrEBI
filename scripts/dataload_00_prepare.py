
import json
import os
import glob
import sys
import subprocess
import shlex
import re
import time

def main():

    config_filename = os.path.abspath(os.path.join('./configs/pipeline_configs/', os.environ['GREBI_CONFIG'] + '.json'))

    with open(config_filename, 'r') as f:
        config = json.load(f)

    datasources = map(lambda x: json.load(open(x, 'r')), config['datasource_configs'])
    datasource_files = []

    for datasource in datasources:
        if datasource['enabled'] != True:
            print("Skipping disabled datasource: " + datasource['name'])
        else:
            for ingest in datasource['ingests']:
                for g in ingest['ingest_files']:
                    files = glob.glob(g)
                    for file in files:
                        filename = os.path.abspath(file)
                        basename = os.path.splitext(os.path.basename(filename))[0]
                        nodes_jsonl_gz_filename = os.path.abspath( os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], '01_ingest', datasource['name'], basename + '.jsonl.gz' ))
                        equivalences_tsv_filename = os.path.abspath( os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], '01_ingest', datasource['name'], basename  + '.equivalences.tsv'  ))
                        expanded_subjects_jsonl_filename = os.path.abspath( os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], '02_equivalences', datasource['name'], basename + '.expanded.jsonl' ))
                        sorted_expanded_subjects_jsonl_filename = os.path.abspath( os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], '02_equivalences', datasource['name'], basename + '.sorted_expanded.jsonl' ))
                        sorted_expanded_subjects_jsonl_gz_filename = os.path.abspath( os.path.join(os.environ['GREBI_HPS_TMP'], os.environ['GREBI_CONFIG'], '02_equivalences', datasource['name'], basename + '.sorted_expanded.jsonl.gz' ))
                        datasource_files.append(json.dumps({
                            'config': config_filename,
                            'datasource': datasource,
                            'ingest': ingest,
                            'filename': filename,
                            'artefacts': {
                                'nodes_jsonl_gz': nodes_jsonl_gz_filename,
                                'equivalences_tsv': equivalences_tsv_filename,
                                'expanded_subjects_jsonl': expanded_subjects_jsonl_filename,
                                'sorted_expanded_subjects_jsonl': sorted_expanded_subjects_jsonl_filename,
                                'sorted_expanded_subjects_jsonl_gz': sorted_expanded_subjects_jsonl_gz_filename
                            }
                        }))

    print("Found " + str(len(datasource_files)) + " files to ingest")

    datasource_files_listing = os.path.abspath( os.path.join(os.environ['GREBI_NFS_TMP'], os.environ['GREBI_CONFIG'], '01_ingest', 'datasource_files.jsonl') )
    os.makedirs(os.path.dirname(datasource_files_listing), exist_ok=True)

    with open(datasource_files_listing, 'w') as f2:
        f2.write('\n'.join(datasource_files))
    print("Files listing written to " + datasource_files_listing)

def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
   main()





