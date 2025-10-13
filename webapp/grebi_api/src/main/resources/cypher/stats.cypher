RETURN {
  num_edges: (
    COUNT { MATCH ()-[r]->() RETURN r } - COUNT { MATCH ()-[r:sourceId]->() RETURN r }
  ),
  num_nodes: COUNT { MATCH (n:GraphNode) RETURN n }
}