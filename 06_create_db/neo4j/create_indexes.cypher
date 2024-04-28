

CREATE FULLTEXT INDEX node_labels FOR (n:GraphNode) ON EACH [n.`ols:label`,n.`impc:name`,n.`monarch:name`]
;
CREATE INDEX node_id FOR (n:GraphNode) ON n.`grebi:nodeId`
;
CALL db.awaitIndexes(10800)
;
