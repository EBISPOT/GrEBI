MATCH (d:disease)<-[r1:`gwas:associated_with`]-(s:`gwas:SNP`)
WHERE d.`grebi:nodeId` = 'mondo:0005044'
WITH d,s,r1
MATCH (s)-[r2]-(o:`otar:Evidence`)
WHERE o.`otar:variantEffect` is not null
WITH d,s, o,r1,r2
ORDER BY o.`otar:score` DESC
WITH d,s, COLLECT(o)[..1] as os,r1, COLLECT(r2)[..1] as r2s
MATCH (s)-[r3]-(g:`hgnc:Gene`)
return distinct(g.`hgnc:symbol`[0]) as gene_symbol, d.`grebi:name`[0] as disease_name, os[0].`otar:variantEffect`[0] as variant_effect, toFloat(os[0].`otar:score`[0]) as otar_score
LIMIT 10