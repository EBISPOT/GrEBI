MATCH (g:`impc:MouseGene`)-[:`biolink:has_phenotype`]->(phenotype:`ols:Class`)-[:`upheno:0000001`]->(anatomical_entity:`ols:Class`)-[:`bfo:part_of`]->(system:`ols:Class`)
WHERE "uberon:0000467" IN system.`ols:directAncestor`
RETURN g.`impc:name`[0] AS gene, system.`ols:label`[0] AS system, count(phenotype) as n_phenotype
ORDER BY n_phenotype DESC
