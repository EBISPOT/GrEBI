
import json
import os
import glob
import time
import yaml

def main():

    print("subgraph is " + os.environ['GREBI_SUBGRAPH'])

    config_filename = os.path.abspath(os.path.join(os.environ['GREBI_HOME'], 'configs/subgraph_configs/', os.environ['GREBI_SUBGRAPH'] + '.json'))

    with open(config_filename, 'r') as f:
        config = json.load(f)

    datasources = map(lambda x: yaml.load(open(os.path.join(os.environ['GREBI_HOME'], x), 'r'), Loader=yaml.FullLoader), config['datasource_configs'])
    datasource_files = []

    for datasource in datasources:
        if datasource['enabled'] != True:
            print("Skipping disabled datasource: " + datasource['name'])
        else:
            for ingest in datasource['ingests']:
                for g in ingest['globs']:
                    files = glob.glob(os.path.join(os.environ['GREBI_HOME'], g))
                    for file in files:
                        filename = os.path.abspath(file)
                        datasource_files.append(json.dumps({
                            'config': config_filename,
                            'datasource': datasource,
                            'ingest': ingest,
                            'filename': filename,
                        }))

    print("Found " + str(len(datasource_files)) + " files to ingest")

    datasource_files_listing = 'datasource_files.jsonl'
    with open(datasource_files_listing, 'w') as f2:
        f2.write('\n'.join(datasource_files))
    print("Files listing written to " + datasource_files_listing)

def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

if __name__=="__main__":
   main()





