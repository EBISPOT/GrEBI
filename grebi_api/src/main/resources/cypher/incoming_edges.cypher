MATCH (other:GraphNode)-[edge]->(n:GraphNode { `grebi:nodeId`: $nodeId })
RETURN other.`grebi:nodeId` as otherId, edge.edge_id as edgeId
SKIP $offset
LIMIT $limit



