MATCH (n:GraphNode { `grebi:nodeId`: $nodeId })-[edge]->(other:GraphNode)
RETURN edge.edge_id as edgeId, other.`grebi:nodeId` as otherId
SKIP $offset
LIMIT $limit

