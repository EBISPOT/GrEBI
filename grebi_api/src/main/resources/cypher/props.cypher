CALL db.relationshipTypes() YIELD relationshipType
UNWIND relationshipType AS t
WITH t
MATCH (n:GraphNode { `grebi:nodeId`: t })
RETURN n.`_json`
