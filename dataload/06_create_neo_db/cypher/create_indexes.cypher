
CREATE INDEX node_id FOR (n:GraphNode) ON n.`grebi:nodeId`
;
CREATE INDEX subgraph FOR (n:GraphNode) ON n.`grebi:subgraph`
;
CREATE INDEX id_id FOR (n:Id) ON n.`id`
;
CREATE INDEX ic FOR (n:GraphNode) ON (n.ic)
;
CREATE VECTOR INDEX embeddings IF NOT EXISTS
FOR (n:GraphNode) ON n.`grebi:embeddingVector` OPTIONS { indexConfig: {
 `vector.dimensions`: 1536,
 `vector.similarity_function`: 'cosine'
}}
;
CALL db.awaitIndexes(86400)
;


