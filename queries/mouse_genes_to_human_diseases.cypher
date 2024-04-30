MATCH (mouse_gene:`impc:MouseGene`)-[:`impc:humanGeneOrthologues`]->(human_gene:`hgnc:Gene`)<-[:`otar:targetId`]-(evidence:`otar:Evidence`)-[:`otar:diseaseId`]->(disease:`ols:Class`)
RETURN mouse_gene.`impc:name`[0] as mouse_gene_name, disease.`ols:label`[0] as disease, evidence.`otar:score`[0] as score
ORDER BY score DESC
