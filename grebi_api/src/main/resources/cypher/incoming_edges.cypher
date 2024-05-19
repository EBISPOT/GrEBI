MATCH (other:GraphNode)-[edge:*]->(n:GraphNode { `grebi:nodeId`=$nodeId })
RETURN other.`grebi:nodeId` as otherId, edge.`grebi:edgeId` as edgeId

