

from pandas import DataFrame
import pandas as pd
import networkx as nx



def aggr_counts(graph, id):
    df = DataFrame(graph.run("""
    MATCH p = (id:Id { id: $id })<-[:id]-(n:GraphNode)<-[ra]-(n1)<-[rb]-(n2)
    WHERE n <> n1 AND n <> n2 AND n1 <> n2 AND type(ra) <> "id" AND type(rb) <> "id"
    RETURN "in_in" AS direction, type(ra) as edge1, n1.`grebi:displayType` AS type1, type(rb) as edge2, n2.`grebi:displayType` as type2, count(p) as num
    UNION
    MATCH p = (id:Id { id: $id })<-[:id]-(n:GraphNode)<-[ra]-(n1)-[rb]->(n2)
    WHERE n <> n1 AND n <> n2 AND n1 <> n2 AND type(ra) <> "id" AND type(rb) <> "id"
    RETURN "in_out" AS direction, type(ra) as edge1, n1.`grebi:displayType` AS type1, type(rb) as edge2, n2.`grebi:displayType` as type2, count(p) as num
    UNION
    MATCH p = (id:Id { id: $id })<-[:id]-(n:GraphNode)-[ra]->(n1)-[rb]->(n2)
    WHERE n <> n1 AND n <> n2 AND n1 <> n2 AND type(ra) <> "id" AND type(rb) <> "id"
    RETURN "out_out" AS direction, type(ra) as edge1, n1.`grebi:displayType` AS type1, type(rb) as edge2, n2.`grebi:displayType` as type2, count(p) as num
    UNION
    MATCH p = (id:Id { id: $id })<-[:id]-(n:GraphNode)-[ra]->(n1)<-[rb]-(n2)
    WHERE n <> n1 AND n <> n2 AND n1 <> n2 AND type(ra) <> "id" AND type(rb) <> "id"
    RETURN "out_in" AS direction, type(ra) as edge1, n1.`grebi:displayType` AS type1, type(rb) as edge2, n2.`grebi:displayType` as type2, count(p) as num
    """,
    {'id': id}
    ).data())
    df['edge1'] = df['edge1'].str.replace("\"", "")
    df['edge2'] = df['edge2'].str.replace("\"", "")
    df['type1'] = df['type1'].str.replace("\"", "")
    df['type2'] = df['type2'].str.replace("\"", "")
    df['path'] = df.apply(path, axis=1)
    return df

def path(row):
    edge1 = "?" if pd.isna(row['edge1']) else row['edge1']
    type1 = "?" if pd.isna(row['type1']) else row['type1']
    edge2 = "?" if pd.isna(row['edge2']) else row['edge2']
    type2 = "?" if pd.isna(row['type2']) else row['type2']
    if row['direction'] == 'out_out':
        return f'(R)-[{edge1}]->({type1})-[{edge2}]->({type2})'
    if row['direction'] == 'in_in':
        return f'(R)<-[{edge1}]-({type1})<-[{edge2}]-({type2})'
    if row['direction'] == 'out_in':
        return f'(R)-[{edge1}]->({type1})<-[{edge2}]-({type2})'
    if row['direction'] == 'in_out':
        return f'(R)<-[{edge1}]-({type1})-[{edge2}]->({type2})'
    
def aggr_counts_to_networkx(df):
    G = nx.DiGraph()
    G.add_node("root", label="root", type='root')
    for i, row in df.iterrows():
        edge1 = "?" if pd.isna(row['edge1']) else row['edge1'].replace("\"", "")
        type1 = "?" if pd.isna(row['type1']) else row['type1'].replace("\"", "")
        edge2 = "?" if pd.isna(row['edge2']) else row['edge2'].replace("\"", "")
        type2 = "?" if pd.isna(row['type2']) else row['type2'].replace("\"", "")
        if row['direction'] == 'out_out':
            middle_node_id = f'(R)-[{edge1}]->({type1})'
            G.add_node(middle_node_id, label=type1)
            G.add_edge("root", middle_node_id, label=edge1)
            end_node_id = f'{middle_node_id}-[{edge2}]->({type2})'
            G.add_node(end_node_id, label=type2, num=int(row['num']), size=row['size'])
            G.add_edge(middle_node_id, end_node_id, label=edge2)
        if row['direction'] == 'in_in':
            middle_node_id = f'(R)<-[{edge1}]-({type1})'
            G.add_node(middle_node_id, label=type1)
            G.add_edge(middle_node_id, "root", label=edge1)
            end_node_id = f'{middle_node_id}<-[{edge2}]-({type2})'
            G.add_node(end_node_id, label=type2, num=int(row['num']), size=row['size'])
            G.add_edge(end_node_id, middle_node_id, label=edge2)
        if row['direction'] == 'out_in':
            middle_node_id = f'(R)-[{edge1}]->({type1})'
            G.add_node(middle_node_id, label=type1)
            G.add_edge("root", middle_node_id, label=edge1)
            end_node_id = f'{middle_node_id}<-[{edge2}]-({type2})'
            G.add_node(end_node_id, label=type2, num=int(row['num']), size=row['size'])
            G.add_edge(end_node_id, middle_node_id, label=edge2)
        if row['direction'] == 'in_out':
            middle_node_id = f'(R)<-[{edge1}]-({type1})'
            G.add_node(middle_node_id, label=type1)
            G.add_edge(middle_node_id, "root", label=edge1)
            end_node_id = f'{middle_node_id}-[{edge2}]->({type2})'
            G.add_node(end_node_id, label=type2, num=int(row['num']), size=row['size'])
            G.add_edge(middle_node_id, end_node_id, label=edge2)        
    return G