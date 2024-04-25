MATCH (allele1:`impc:Allele`)<-[:`impc:alleles`]-(pub:`impc:Publication`)-[:`impc:alleles`]->(allele2:`impc:Allele`)
WHERE elementId(allele1) < elementId(allele2)
WITH allele1, allele2, collect(DISTINCT pub) AS common_pubs
RETURN
    allele1.`impc:name`[0] AS a1,
    allele2.`impc:name`[0] AS a2,
    size(common_pubs) AS num_common_pubs,
    COLLECT {
        MATCH (allele1)<-[:`impc:mouseAlleleId`]-(assoc1:`impc:GenePhenotypeAssociation`)-[:`biolink:has_phenotype`]-(phenotype1:`ols:Class`)
        RETURN phenotype1.`ols:label`[0]
    } AS p1,
    COLLECT {
        MATCH (allele2)<-[:`impc:mouseAlleleId`]-(assoc2:`impc:GenePhenotypeAssociation`)-[:`biolink:has_phenotype`]-(phenotype2:`ols:Class`)
        RETURN phenotype2.`ols:label`[0]
    } AS p2
ORDER BY num_common_pubs DESC
LIMIT 10

