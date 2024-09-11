MATCH (d:`biolink:Disease`)-[:id]->(id:Id {id: "mondo:0005044"})
WITH d
MATCH (d)<-[r1:`gwas:associated_with`]-(s:`gwas:SNP`)-[]->(g:`hgnc:Gene`)
WITH d,s,r1,g
MATCH (s)-[r2]-(o:`otar:Evidence`)
WHERE o.`otar:variantEffect` is not null
WITH d,s, o,r1,r2,g
ORDER BY o.`otar:score` DESC
RETURN DISTINCT(g.`hgnc:symbol`[0]) as gene_symbol, d.`grebi:name`[0] as disease_name, o.`otar:variantEffect`[0] as variant_effect, toFloat(o.`otar:score`[0]) as otar_score
LIMIT 10