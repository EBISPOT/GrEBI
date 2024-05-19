MATCH (n:GraphNode { `grebi:nodeId`=$nodeId })-[edge:*]->(other:GraphNode)
RETURN edge.`grebi:edgeId` as edgeId, other.`grebi:nodeId` as otherId

