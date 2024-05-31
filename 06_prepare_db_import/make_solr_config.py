
import json
import os
import sys
import shlex
import time
import glob
import argparse
from pathlib import Path
from subprocess import Popen, PIPE, STDOUT


def main():
    parser = argparse.ArgumentParser(description='Create Solr config')
    parser.add_argument('--in-summary-json', type=str, help='summary.json', required=True)
    parser.add_argument('--in-template-config-dir', type=str, help='Path of config template', required=True)
    parser.add_argument('--out-config-dir', type=str, help='Path to write config', required=True)
    args = parser.parse_args()
   
    os.system('cp -r ' + shlex.quote(args.in_template_config_dir) + ' ' + shlex.quote(args.out_config_dir))

    summary = json.load(open(args.in_summary_json))
    node_props = map(lambda f: f.replace(':', '__'), summary['entity_props'].keys())
    edge_props = map(lambda f: f.replace(':', '__'), summary['edge_props'].keys())

    nodes_schema = Path(os.path.join(args.out_config_dir, 'grebi_nodes/conf/schema.xml'))
    nodes_schema.write_text(nodes_schema.read_text().replace('[[GREBI_FIELDS]]', '\n'.join(list(map(
        lambda f: '\n'.join([
            f'<field name="{f}" type="string" indexed="true" stored="false" required="false" multiValued="true" />',
            f'<copyField source="{f}" dest="str_{f}"/>',
            f'<copyField source="{f}" dest="lowercase_{f}"/>'
        ]), node_props)))))

    edges_schema = Path(os.path.join(args.out_config_dir, 'grebi_edges/conf/schema.xml'))
    edges_schema.write_text(edges_schema.read_text().replace('[[GREBI_FIELDS]]', '\n'.join(list(map(
        lambda f: '\n'.join([
            f'<field name="{f}" type="string" indexed="true" stored="false" required="false" multiValued="true" />',
            f'<copyField source="{f}" dest="str_{f}"/>',
            f'<copyField source="{f}" dest="lowercase_{f}"/>'
        ]), edge_props)))))

if __name__=="__main__":
    main()


