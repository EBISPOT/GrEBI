
CREATE INDEX node_id FOR (n:GraphNode) ON n.`grebi:nodeId`
;
CREATE INDEX subgraph FOR (n:GraphNode) ON n.`grebi:subgraph`
;
CREATE INDEX id_id FOR (n:Id) ON n.`id`
;
CALL db.awaitIndexes(10800)
;
