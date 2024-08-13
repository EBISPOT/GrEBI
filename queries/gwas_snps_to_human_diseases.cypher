MATCH (d:`biolink:Disease`)
WHERE d.`grebi:nodeId` = 'mondo:0005044'
WITH d
MATCH (s:`gwas:SNP`)-[]->(d)
WITH s, d
MATCH p = (d)<-[]-(s)-[]->(g:`hgnc:Gene`)
return p