#!/bin/bash 

mkdir export

docker run \
	-v $(pwd)/reactome.graphdb.dump:/dump \
	-v $(pwd)/export:/var/lib/neo4j/import:rw \
	-e NEO4J_apoc_export_file_enabled=true \
	-e NEO4J_apoc_import_file_enabled=true \
	-e NEO4J_apoc_import_file_use__neo4j__config=true \
	-e NEO4J_PLUGINS=\[\"apoc\"\] \
	-p 7474:7474 \
	-p 7687:7687 \
	neo4j:5.15.0 \
	bash -c "cat /dump | neo4j-admin database load --from-stdin neo4j && \
	neo4j-admin database migrate --force-btree-indexes-to-range neo4j && \
	echo \"dbms.security.auth_enabled=false\" >> /var/lib/neo4j/conf/neo4j.conf && \
	neo4j-admin server start &&
	sleep 15 && \
	cypher-shell -u neo4j -p neo4j \"CALL apoc.export.json.all(\\\"reactome.json\\\",{useTypes:true})\"
	"
