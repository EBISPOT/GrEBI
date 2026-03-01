
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
    parser.add_argument('--subgraph-name', type=str, help='subgraph name', required=True)
    parser.add_argument('--in-graph-metadata-json', type=str, help='summary.json', required=True)
    parser.add_argument('--in-template-config-dir', type=str, help='Path of config template', required=True)
    parser.add_argument('--out-config-dir', type=str, help='Path to write config', required=True)
    args = parser.parse_args()
   
    os.makedirs(args.out_config_dir, exist_ok=True)

    nodes_core_path = os.path.join(args.out_config_dir, f'grebi_nodes_{args.subgraph_name}')
    edges_core_path = os.path.join(args.out_config_dir, f'grebi_edges_{args.subgraph_name}')
    os.system('cp -r ' + shlex.quote(os.path.join(args.in_template_config_dir, "grebi_nodes")) + ' ' + shlex.quote(nodes_core_path))
    os.system('cp -r ' + shlex.quote(os.path.join(args.in_template_config_dir, "grebi_edges")) + ' ' + shlex.quote(edges_core_path))

    os.system('cp ' + shlex.quote(os.path.join(args.in_template_config_dir, "solr.xml")) + ' ' + shlex.quote(args.out_config_dir))
    os.system('cp ' + shlex.quote(os.path.join(args.in_template_config_dir, "solrconfig.xml")) + ' ' + shlex.quote(args.out_config_dir))
    os.system('cp ' + shlex.quote(os.path.join(args.in_template_config_dir, "zoo.cfg")) + ' ' + shlex.quote(args.out_config_dir))

    summary = json.load(open(args.in_graph_metadata_json))

    entity_props_not_embeddings = list(filter(lambda p: not p.startswith('embedding:'), summary['entity_props'].keys()))
    entity_props_embeddings = list(filter(lambda p: p.startswith('embedding:'), summary['entity_props'].keys()))
    embedding_models2dims = summary.get('embedding_models2dims', {})

    node_props = list(map(lambda f: f.replace(':', '__').replace('&', '_'), entity_props_not_embeddings))
    node_props_embeddings = list(map(lambda f: f.replace(':', '__').replace('&', '_'), entity_props_embeddings))
    edge_props = list(map(lambda f: f.replace(':', '__').replace('&', '_'), summary['edge_props'].keys()))

    # The API hardcodes these fields in its edismax qf (search) queries, so they
    # must always exist in the Solr schema even if no documents contain them.
    api_required_node_fields = ['id', 'grebi__name', 'grebi__synonym', 'grebi__description', 'grebi__type']
    for f in api_required_node_fields:
        if f not in node_props:
            node_props.append(f)

    Path(f'{nodes_core_path}/core.properties').write_text(f"name=grebi_nodes_{args.subgraph_name}\n")
    Path(f'{edges_core_path}/core.properties').write_text(f"name=grebi_edges_{args.subgraph_name}\n")

    nodes_schema = Path(f'{nodes_core_path}/conf/schema.xml')
    nodes_schema.write_text(nodes_schema.read_text().replace('[[GREBI_FIELDS]]', '\n'.join(
        list(
            map(lambda f: '\n'.join([
                f'<field name="{f}" type="string" indexed="true" stored="false" required="false" multiValued="true" />',
                f'<copyField source="{f}" dest="str_{f}"/>',
                f'<copyField source="{f}" dest="lowercase_{f}"/>'
            ]), node_props)
        )
        +
        list(
            map(
                lambda f: (lambda model_id: '\n'.join([
                    f'<fieldType name="knn_vector_{model_id}" class="solr.DenseVectorField" vectorDimension="{embedding_models2dims.get(model_id, "")}" similarityFunction="cosine"/>',
                    f'<field name="embedding__{model_id}" type="knn_vector_{model_id}" indexed="true" stored="true"/>'
                ]))(f.split('__')[1]),
                node_props_embeddings
            )
        )
    )))

#    sb.append("    <fieldType name=\"knn_vector_" + modelName + "\" class=\"solr.DenseVectorField\" vectorDimension=\"" + embeddingVectorSize + "\" similarityFunction=\"cosine\"/>\n");
#             sb.append("    <field name=\"embeddings_" + modelName + "\" type=\"knn_vector_" + modelName + "\" indexed=\"true\" stored=\"true\"/>\n");
#         }

    edges_schema = Path(f'{edges_core_path}/conf/schema.xml')
    edges_schema.write_text(edges_schema.read_text().replace('[[GREBI_FIELDS]]', '\n'.join(list(map(
        lambda f: '\n'.join([
            f'<field name="{f}" type="string" indexed="true" stored="false" required="false" multiValued="true" />',
            f'<copyField source="{f}" dest="str_{f}"/>',
            f'<copyField source="{f}" dest="lowercase_{f}"/>'
        ]), edge_props)))))

if __name__=="__main__":
    main()


