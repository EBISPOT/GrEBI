package uk.ac.ebi.grebi.repo;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrebiQueryTemplatesRepoTest {

    @Test
    void rewritesDoubleQuotedDatasourcePredicateToAcceptBothNamings() {
        var out = GrebiQueryTemplatesRepo.normaliseOntologyDatasourceNames(
                "WHERE \"Ontologies.mp\" IN any_term.`grebi:datasources`");

        assertEquals("WHERE any(__grebi_ds IN any_term.`grebi:datasources` " +
                "WHERE __grebi_ds IN [\"Ontologies.mp\", \"OLS.mp\"])", out);
    }

    @Test
    void rewritesSingleQuotedPredicateToo() {
        var out = GrebiQueryTemplatesRepo.normaliseOntologyDatasourceNames(
                "WHERE 'Ontologies.mondo' IN disease.`grebi:datasources`");

        assertTrue(out.contains("[\"Ontologies.mondo\", \"OLS.mondo\"]"), out);
        assertTrue(out.contains("any(__grebi_ds IN disease.`grebi:datasources`"), out);
    }

    // A template authored against the live OLS.* naming must survive the
    // changeover just as one authored against Ontologies.* does.
    @Test
    void rewritesOlsNamingSymmetrically() {
        var out = GrebiQueryTemplatesRepo.normaliseOntologyDatasourceNames(
                "WHERE \"OLS.hp\" IN phenotype.`grebi:datasources`");

        assertTrue(out.contains("[\"Ontologies.hp\", \"OLS.hp\"]"), out);
    }

    @Test
    void rewritesEveryPredicateInAFragment() {
        var out = GrebiQueryTemplatesRepo.normaliseOntologyDatasourceNames(
                "WHERE \"Ontologies.mp\" IN mousePhenotype.`grebi:datasources` " +
                        "AND \"Ontologies.hp\" IN humanPhenotype.`grebi:datasources`");

        assertTrue(out.contains("mousePhenotype.`grebi:datasources` WHERE __grebi_ds IN " +
                "[\"Ontologies.mp\", \"OLS.mp\"]"), out);
        assertTrue(out.contains("humanPhenotype.`grebi:datasources` WHERE __grebi_ds IN " +
                "[\"Ontologies.hp\", \"OLS.hp\"]"), out);
        assertTrue(out.indexOf("IN mousePhenotype") < out.indexOf("IN humanPhenotype"), out);
    }

    @Test
    void leavesUnrelatedCypherAlone() {
        var cypher = "MATCH (snp:`gwas:SNP`)-[assoc:`gwas:associated_with`]->(trait)\n" +
                "WHERE 'GWAS' IN snp.`grebi:datasources`";

        assertEquals(cypher, GrebiQueryTemplatesRepo.normaliseOntologyDatasourceNames(cypher));
    }

    @Test
    void toleratesNullFragments() {
        assertNull(GrebiQueryTemplatesRepo.normaliseOntologyDatasourceNames(null));
    }

    // The rewrite emits both spellings as literals; re-running it must not
    // nest a second time around them.
    @Test
    void isIdempotent() {
        var once = GrebiQueryTemplatesRepo.normaliseOntologyDatasourceNames(
                "WHERE \"Ontologies.mp\" IN any_term.`grebi:datasources`");
        var twice = GrebiQueryTemplatesRepo.normaliseOntologyDatasourceNames(once);

        assertEquals(once, twice);
    }
}
