
CREATE INDEX node_id FOR (n:GraphNode) ON n.`grebi:nodeId`
;
CREATE INDEX subgraph FOR (n:GraphNode) ON n.`grebi:subgraph`
;
CALL db.awaitIndexes(10800)
;
