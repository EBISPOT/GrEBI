package uk.ac.ebi.grebi.repo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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

    // ------------------------------------------------------------------
    // Loading is per file: one bad template must not take the rest down.
    // ------------------------------------------------------------------

    private static final String GOOD = "title: Good\nquestion: q\ntopics: [t]\nresult_columns: []\n";

    private static void write(Path dir, String name, String yaml) throws IOException {
        Files.createDirectories(dir);
        Files.writeString(dir.resolve(name), yaml);
    }

    @Test
    void skipsATemplateWithAnUnknownKeyAndKeepsTheRest(@TempDir Path dir) throws IOException {
        write(dir, "a.yaml", GOOD);
        write(dir, "b.yaml", GOOD + "params:\n  - param_id: x\n    no_such_key: 1\n");
        write(dir, "c.yaml", GOOD);

        List<QueryTemplate> loaded = GrebiQueryTemplatesRepo.loadQueryTemplates(dir.toString());

        assertEquals(List.of("a", "c"), loaded.stream().map(t -> t.id).toList());
    }

    @Test
    void skipsMalformedYaml(@TempDir Path dir) throws IOException {
        write(dir, "a.yaml", GOOD);
        write(dir, "broken.yaml", "title: [unclosed\n");

        List<QueryTemplate> loaded = GrebiQueryTemplatesRepo.loadQueryTemplates(dir.toString());

        assertEquals(List.of("a"), loaded.stream().map(t -> t.id).toList());
    }

    @Test
    void skipsTheSecondOfADuplicateIdAcrossSubdirectories(@TempDir Path dir) throws IOException {
        write(dir.resolve("x"), "dup.yaml", GOOD);
        write(dir.resolve("y"), "dup.yaml", GOOD);
        write(dir, "a.yaml", GOOD);

        List<QueryTemplate> loaded = GrebiQueryTemplatesRepo.loadQueryTemplates(dir.toString());

        assertEquals(List.of("a", "dup"), loaded.stream().map(t -> t.id).toList());
    }

    @Test
    void underscoreFilesAreNotTemplates(@TempDir Path dir) throws IOException {
        write(dir, "_topics.yaml", "topics: []\n");
        write(dir, "a.yaml", GOOD);

        List<QueryTemplate> loaded = GrebiQueryTemplatesRepo.loadQueryTemplates(dir.toString());

        assertEquals(List.of("a"), loaded.stream().map(t -> t.id).toList());
    }

    @Test
    void repoServesEmptyListsRatherThanNullWhenTheDirectoryIsMissing(@TempDir Path dir) {
        var repo = new GrebiQueryTemplatesRepo(dir.resolve("nope").toString());

        assertEquals(List.of(), repo.getQueryTemplates());
        assertEquals(List.of(), repo.getQueryTopics());
    }

    // ------------------------------------------------------------------
    // Change detection is by stat walk (the directory is NFS in production).
    // ------------------------------------------------------------------

    @Test
    void fingerprintChangesWhenAFileIsAddedEditedOrRemoved(@TempDir Path dir) throws IOException {
        write(dir, "a.yaml", GOOD);
        String initial = GrebiQueryTemplatesRepo.fingerprint(dir);

        write(dir.resolve("sub"), "b.yaml", GOOD);
        String added = GrebiQueryTemplatesRepo.fingerprint(dir);
        assertNotEquals(initial, added);

        // Same size, different content: mtime must carry the change.
        Files.setLastModifiedTime(dir.resolve("a.yaml"),
                java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() + 5000));
        String edited = GrebiQueryTemplatesRepo.fingerprint(dir);
        assertNotEquals(added, edited);

        Files.delete(dir.resolve("sub").resolve("b.yaml"));
        assertNotEquals(edited, GrebiQueryTemplatesRepo.fingerprint(dir));
    }

    @Test
    void fingerprintIsStableWhenNothingChanged(@TempDir Path dir) throws IOException {
        write(dir, "a.yaml", GOOD);
        assertEquals(GrebiQueryTemplatesRepo.fingerprint(dir), GrebiQueryTemplatesRepo.fingerprint(dir));
    }

    // ------------------------------------------------------------------
    // materialise: materialised by default; `false` is the only opt-out.
    // ------------------------------------------------------------------

    private static final String PARAMETERISED = "title: T\nparams:\n  - param_id: x_id\n    param_type: SourceId\nresult_columns: []\n";

    private static QueryTemplate loadOne(Path dir, String yaml) throws IOException {
        write(dir, "t.yaml", yaml);
        var loaded = GrebiQueryTemplatesRepo.loadQueryTemplates(dir.toString());
        return loaded.isEmpty() ? null : loaded.get(0);
    }

    @Test
    void aTemplateIsMaterialisedByDefault(@TempDir Path dir) throws IOException {
        var t = loadOne(dir, PARAMETERISED);
        assertTrue(t.materialised);
        assertTrue(t.isParameterisedMaterialised());
        assertNull(t.materialise);
    }

    @Test
    void materialiseFalseOptsOut(@TempDir Path dir) throws IOException {
        var t = loadOne(dir, PARAMETERISED + "materialise: false\n");
        assertFalse(t.materialised);
        assertFalse(t.isParameterisedMaterialised());
        assertFalse(t.isStandaloneMaterialised());
    }

    @Test
    void aSettingsBlockKeepsItMaterialised(@TempDir Path dir) throws IOException {
        var t = loadOne(dir, PARAMETERISED + "materialise:\n  budget_rows: 20000000\n  allow_empty: true\n");
        assertTrue(t.materialised);
        assertEquals(20_000_000, t.materialise.budget_rows);
        assertTrue(t.materialise.allow_empty);
    }

    @Test
    void aStandaloneCypherBodyIsMaterialised(@TempDir Path dir) throws IOException {
        var t = loadOne(dir, "title: T\nresult_columns: []\nmaterialise:\n  cypher: RETURN 1 AS a\n");
        assertTrue(t.materialised);
        assertTrue(t.isStandaloneMaterialised());
    }

    @Test
    void noParamsAndNoCypherIsNotMaterialised(@TempDir Path dir) throws IOException {
        var t = loadOne(dir, "title: T\nresult_columns: []\n");
        assertFalse(t.materialised);
        assertFalse(t.isStandaloneMaterialised());
    }

    @Test
    void theRemovedModeSettingAndUnknownSettingsAreRejected(@TempDir Path dir) throws IOException {
        // Rejected templates are skipped (and logged), not loaded half-understood.
        assertNull(loadOne(dir, PARAMETERISED + "materialise:\n  mode: full\n"));
        assertNull(loadOne(dir, PARAMETERISED + "materialise:\n  mode: counts_only\n"));
        assertNull(loadOne(dir, PARAMETERISED + "materialise:\n  nonsense: 1\n"));
        assertNull(loadOne(dir, PARAMETERISED + "materialise: true\n"));
    }
}
