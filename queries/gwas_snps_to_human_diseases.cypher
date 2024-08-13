MATCH (d:`biolink:Disease`)-[:id]->(id:Id {id: "mondo:0005044"})
WITH d
MATCH (s:`gwas:SNP`)-[]->(d)
WITH s, d
MATCH p = (d)<-[]-(s)-[]->(g:`hgnc:Gene`)
RETURN p