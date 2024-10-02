

from pandas import DataFrame
import pandas as pd
import networkx as nx

def query_to_nx(driver, query):
    results = driver.session().run(query)
    G = nx.MultiDiGraph()
    for node in list(results.graph()._nodes.values()):
        if not 'Id' in node._labels:
            G.add_nodes_from([(node._properties['grebi:nodeId'], dict({"grebi:type": list(node._labels)}, **node._properties))])
    for rel in list(results.graph()._relationships.values()):
        if not 'Id' in rel.start_node._labels and not 'Id' in rel.end_node._labels:
            G.add_edges_from([(rel.start_node._properties['grebi:nodeId'], rel.end_node._properties['grebi:nodeId'], rel._properties['edge_id'], dict({"grebi:type": rel.type}, **rel._properties))])
    return G

def nx_to_cytoscape(G):
    cydata = nx.cytoscape_data(G)
    for node in cydata['elements']['nodes']:
        node['data']['ids'] = node['data']['id']
        node['data']['id'] = node['data']['grebi:nodeId']
        del node['data']['grebi:nodeId']
    return cydata