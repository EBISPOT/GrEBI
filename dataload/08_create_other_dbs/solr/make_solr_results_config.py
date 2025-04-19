import os
import shlex
import argparse
from pathlib import Path
from subprocess import Popen, PIPE, STDOUT

def main():
    parser = argparse.ArgumentParser(description='Create Solr results config')
    parser.add_argument('--subgraph-name', type=str, help='subgraph name', required=True)
    parser.add_argument('--query-id', type=str, help='query id', required=True)
    parser.add_argument('--in-template-config-dir', type=str, help='Path of config template', required=True)
    parser.add_argument('--out-config-dir', type=str, help='Path to write config', required=True)
    args = parser.parse_args()
   
    os.makedirs(args.out_config_dir, exist_ok=True)

    results_core_path = os.path.join(args.out_config_dir, f'grebi_results__{args.subgraph_name}__{args.query_id}')
    os.system('cp -r ' + shlex.quote(os.path.join(args.in_template_config_dir, "grebi_results")) + ' ' + shlex.quote(results_core_path))

    os.system('cp ' + shlex.quote(os.path.join(args.in_template_config_dir, "solr.xml")) + ' ' + shlex.quote(args.out_config_dir))
    os.system('cp ' + shlex.quote(os.path.join(args.in_template_config_dir, "solrconfig.xml")) + ' ' + shlex.quote(args.out_config_dir))
    os.system('cp ' + shlex.quote(os.path.join(args.in_template_config_dir, "zoo.cfg")) + ' ' + shlex.quote(args.out_config_dir))

    Path(f'{results_core_path}/core.properties').write_text(f"name=grebi_results__{args.subgraph_name}__{args.query_id}\n")

if __name__=="__main__":
    main()




