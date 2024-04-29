
CREATE INDEX node_id FOR (n:GraphNode) ON n.`grebi:nodeId`
;
CALL db.awaitIndexes(10800)
;
