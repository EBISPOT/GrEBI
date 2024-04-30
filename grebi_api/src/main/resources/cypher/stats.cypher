RETURN {
  num_edges: COLLECT { MATCH ()-[r]->() RETURN count(*) }[0],
  num_nodes: COLLECT { MATCH (n) RETURN count(*) }[0]
}


