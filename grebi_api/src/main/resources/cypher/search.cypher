CALL db.index.fulltext.queryNodes("node_labels", $query, { skip: $skip, limit: $limit })
YIELD node, score
RETURN node
ORDER BY score DESC
