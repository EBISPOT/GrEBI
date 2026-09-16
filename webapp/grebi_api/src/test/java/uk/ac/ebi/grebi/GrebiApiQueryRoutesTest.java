package uk.ac.ebi.grebi;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import uk.ac.ebi.grebi.db.MaterialisedBuild;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Query templates, query runs (live, materialised, counts-only, CSV) and materialised queries. */
class GrebiApiQueryRoutesTest {

    TestApp app;

    @BeforeEach
    void start() {
        app = TestApp.start();
    }

    @AfterEach
    void stop() {
        app.close();
    }

    static List<String> ids(TestApp.Response res) {
        return res.json().getAsJsonArray().asList().stream()
            .map(e -> e.getAsJsonObject().get("id").getAsString()).toList();
    }

    @Test
    void templatesAreListedPerGraphWithoutTheStandaloneQueries() {
        var g1 = app.get("/api/v1/graphs/g1/query_templates");
        assertEquals("no-cache", g1.header("cache-control"));
        // node_count has no graphs list, so it is offered on every graph; all_studies is a
        // standalone materialised query, browsed through /materialised_queries instead
        assertEquals(List.of("node_count", "snps_by_trait_materialised", "studies_by_trait", "study_counts_by_trait"), ids(g1));
        assertEquals(List.of("node_count", "snps_by_trait_materialised"), ids(app.get("/api/v1/graphs/g2/query_templates")));
    }

    @Test
    void aTemplateIsServedForItsGraphsOnly() {
        var res = app.get("/api/v1/graphs/g1/query_templates/studies_by_trait");
        assertEquals(200, res.status());
        var template = res.json().getAsJsonObject();
        assertEquals("GWAS studies annotated with a trait", template.get("title").getAsString());
        assertEquals("trait_id", template.getAsJsonArray("params").get(0).getAsJsonObject().get("param_id").getAsString());

        var wrongGraph = app.get("/api/v1/graphs/g2/query_templates/studies_by_trait");
        assertEquals(404, wrongGraph.status());
        assertEquals("Query template studies_by_trait not found for graph g2",
            wrongGraph.json().getAsJsonObject().get("error").getAsString());
        assertEquals(404, app.get("/api/v1/graphs/g1/query_templates/nope").status());
        assertEquals(404, app.get("/api/v1/graphs/g1/query/nope").status());
    }

    @Test
    @SuppressWarnings("unchecked")
    void aLiveTemplateRunsThroughCypherWithItsParametersSortAndPage() {
        when(app.cypher.runQueryFromTemplatePaginated(eq("g1"), any(), any(), anyBoolean(), any()))
            .thenReturn(new PageImpl<>(List.of(TestApp.row("study", Map.of("id", "GCST1"))), PageRequest.of(0, 5), 1));

        var res = app.get("/api/v1/graphs/g1/query/studies_by_trait?trait_id=mondo:0005083&page=0&size=5&sortBy=study_accession&sortDir=desc");
        assertEquals(200, res.status());
        JsonObject body = res.json().getAsJsonObject();
        assertEquals(1, body.get("totalElements").getAsInt());
        assertEquals("GCST1", body.getAsJsonArray("content").get(0).getAsJsonObject().getAsJsonObject("study").get("id").getAsString());

        var params = ArgumentCaptor.forClass(Map.class);
        var page = ArgumentCaptor.forClass(Pageable.class);
        verify(app.cypher).runQueryFromTemplatePaginated(eq("g1"), argThat(t -> t.id.equals("studies_by_trait")),
            params.capture(), eq(false), page.capture());
        assertEquals(Map.of("trait_id", List.of("mondo:0005083")), params.getValue());
        assertEquals(5, page.getValue().getPageSize());
        assertEquals(Sort.by(Sort.Direction.DESC, "study_accession"), page.getValue().getSort());

        // the default sort is the first result column ascending, and resolve=true is passed on
        app.get("/api/v1/graphs/g1/query/studies_by_trait?trait_id=x&resolve=true");
        verify(app.cypher).runQueryFromTemplatePaginated(eq("g1"), any(), any(), eq(true),
            argThat(p -> p.getSort().equals(Sort.by(Sort.Direction.ASC, "trait"))));
    }

    @Test
    void aLiveTemplateWithoutTheCypherServiceIsA500WithAJsonError() {
        try (var noCypher = TestApp.startWithoutCypher()) {
            var res = noCypher.get("/api/v1/graphs/g1/query/studies_by_trait?trait_id=x");
            assertEquals(500, res.status());
            assertEquals("Cypher service unavailable; cannot serve live query template studies_by_trait",
                res.json().getAsJsonObject().get("error").getAsString());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void aFullyMaterialisedTemplateIsServedFromPostgresWithFacetSelectionsSplitFromParameters() {
        when(app.postgres.runMaterialisedParameterisedPaginated(eq("g1"), any(), any(), any(), any(), any(), anyBoolean(), any()))
            .thenReturn(TestApp.facetedPage(List.of(TestApp.row("snp", Map.of("id", "rs1"))),
                Map.of("trait", Map.of("mondo:0005083", 1L)), 1));

        var res = app.get("/api/v1/graphs/g1/query/snps_by_trait_materialised?trait_id=efo:1&trait=mondo:0005083&q=psor");
        assertEquals(200, res.status());
        var body = res.json().getAsJsonObject();
        assertEquals(1, body.getAsJsonObject("facetFieldToCounts").getAsJsonObject("trait").get("mondo:0005083").getAsInt());

        var build = ArgumentCaptor.forClass(MaterialisedBuild.class);
        var params = ArgumentCaptor.forClass(Map.class);
        var filters = ArgumentCaptor.forClass(Map.class);
        verify(app.postgres).runMaterialisedParameterisedPaginated(eq("g1"), any(), build.capture(), params.capture(),
            eq("psor"), filters.capture(), eq(false), any());
        assertEquals("matq_g1_snps_by_trait", build.getValue().table);
        assertEquals(Map.of("trait_id", List.of("efo:1")), params.getValue(), "the template parameter");
        assertEquals(Map.of("trait", List.of("mondo:0005083")), filters.getValue(), "a result column is a facet selection");
        verify(app.cypher, never()).runQueryFromTemplatePaginated(any(), any(), any(), anyBoolean(), any());
    }

    @Test
    void aMaterialisedTemplateFallsBackToLiveCypherOnAGraphWithoutABuild() {
        when(app.cypher.runQueryFromTemplatePaginated(eq("g2"), any(), any(), anyBoolean(), any()))
            .thenReturn(new PageImpl<>(List.of(), PageRequest.of(0, 10), 0));
        assertEquals(200, app.get("/api/v1/graphs/g2/query/snps_by_trait_materialised?trait_id=efo:1").status());
        verify(app.cypher).runQueryFromTemplatePaginated(eq("g2"), any(), eq(Map.of("trait_id", List.of("efo:1"))), eq(false), any());
        verify(app.postgres, never()).runMaterialisedParameterisedPaginated(any(), any(), any(), any(), any(), any(), anyBoolean(), any());
    }

    @Test
    void aCountsOnlyTemplateTakesItsTotalFromPostgresAndItsRowsFromCypher() {
        when(app.postgres.materialisedParameterisedCount(eq("g1"), any(), any(), any())).thenReturn(77L);
        when(app.cypher.runQueryFromTemplatePaginated(eq("g1"), any(), any(), anyBoolean(), any(), eq(77L)))
            .thenReturn(new PageImpl<>(List.of(TestApp.row("study", Map.of("id", "GCST1"))), PageRequest.of(0, 10), 77));

        var body = app.get("/api/v1/graphs/g1/query/study_counts_by_trait?trait_id=mondo:0005083").json().getAsJsonObject();
        assertEquals(77, body.get("totalElements").getAsInt());
        assertEquals(1, body.getAsJsonArray("content").size());
        verify(app.postgres).materialisedParameterisedCount(eq("g1"), argThat(t -> t.id.equals("study_counts_by_trait")),
            argThat(b -> b.isCountsOnly()), eq(Map.of("trait_id", List.of("mondo:0005083"))));
    }

    @Test
    void csvExportStreamsTheLiveQueryAsAnAttachment() throws Exception {
        when(app.cypher.runQueryFromTemplateStreamed(eq("g1"), any(), any(), any(Sort.class), any(PrintWriter.class)))
            .thenAnswer(inv -> {
                PrintWriter writer = inv.getArgument(4);
                writer.write("trait,study\nmondo:0005083,GCST1\n");
                writer.flush();
                return CompletableFuture.completedFuture(null);
            });

        var res = app.get("/api/v1/graphs/g1/query/studies_by_trait.csv?trait_id=mondo:0005083&sortBy=study&sortDir=desc");
        assertEquals(200, res.status());
        assertTrue(res.header("content-type").startsWith("text/csv"), res.header("content-type"));
        assertEquals("attachment; filename=\"studies_by_trait.csv\"", res.header("content-disposition"));
        assertEquals("trait,study\nmondo:0005083,GCST1\n", res.body());
        verify(app.cypher).runQueryFromTemplateStreamed(eq("g1"), any(), eq(Map.of("trait_id", List.of("mondo:0005083"))),
            eq(Sort.by(Sort.Direction.DESC, "study")), any(PrintWriter.class));
    }

    @Test
    void csvExportOfAMaterialisedTemplateStreamsFromPostgres() throws Exception {
        doAnswer(inv -> {
            PrintWriter writer = inv.getArgument(7);
            writer.write("trait,snp,p_value\nmondo:0005083,rs1,0.5\n");
            writer.flush();
            return null;
        }).when(app.postgres).streamMaterialisedParameterisedCsv(eq("g1"), any(), any(), any(), any(), any(), any(), any());

        var res = app.get("/api/v1/graphs/g1/query/snps_by_trait_materialised.csv?trait_id=efo:1&trait=mondo:0005083&filter=rs");
        assertEquals(200, res.status());
        assertEquals("trait,snp,p_value\nmondo:0005083,rs1,0.5\n", res.body());
        verify(app.postgres).streamMaterialisedParameterisedCsv(eq("g1"), any(), argThat(b -> "matq_g1_snps_by_trait".equals(b.table)),
            eq(Map.of("trait_id", List.of("efo:1"))), eq("rs"), eq(Map.of("trait", List.of("mondo:0005083"))),
            eq(Sort.by(Sort.Direction.ASC, "trait")), any());
        verify(app.cypher, never()).runQueryFromTemplateStreamed(any(), any(), any(), any(Sort.class), any(PrintWriter.class));
    }

    @Test
    void csvExportWithoutTheCypherServiceFails() {
        try (var noCypher = TestApp.startWithoutCypher()) {
            var res = noCypher.get("/api/v1/graphs/g1/query/studies_by_trait.csv?trait_id=x");
            assertEquals(500, res.status());
            assertEquals("Cypher service unavailable; cannot serve CSV for studies_by_trait",
                res.json().getAsJsonObject().get("error").getAsString());
        }
    }

    @Test
    void materialisedQueriesAreListedAcrossGraphsAndPerGraph() {
        var all = app.get("/api/v1/materialised_queries").json().getAsJsonArray();
        assertEquals(1, all.size());
        assertEquals("all_studies", all.get(0).getAsJsonObject().get("id").getAsString());
        assertEquals("g1", all.get(0).getAsJsonObject().get("graph").getAsString(), "each entry says which graph it is from");

        assertEquals(1, app.get("/api/v1/graphs/g1/materialised_queries").json().getAsJsonArray().size());
        assertEquals("[]", app.get("/api/v1/graphs/g2/materialised_queries").body());
    }

    @Test
    @SuppressWarnings("unchecked")
    void materialisedQueryResultsComeFromPostgresWithFiltersAndFacets() {
        when(app.postgres.searchMaterialisedQueryResultsPaginated(any(), any(), any(), any(), any()))
            .thenReturn(TestApp.facetedPage(List.of(TestApp.row("study", Map.of("id", "GCST1"))),
                Map.of("study", Map.of("GCST1", 1L)), 1));

        var res = app.get("/api/v1/graphs/g1/materialised_queries/all_studies?q=psor&facet=study&study=GCST1&page=0&size=20");
        assertEquals(200, res.status());
        var body = res.json().getAsJsonObject();
        assertEquals(1, body.get("totalElements").getAsInt());
        assertEquals(1, body.getAsJsonObject("facetFieldToCounts").getAsJsonObject("study").get("GCST1").getAsInt());

        var filters = ArgumentCaptor.forClass(Map.class);
        var page = ArgumentCaptor.forClass(Pageable.class);
        verify(app.postgres).searchMaterialisedQueryResultsPaginated(argThat(b -> "matq_g1_all_studies".equals(b.table)),
            eq("psor"), filters.capture(), eq(List.of("study")), page.capture());
        assertEquals(Map.of("study", List.of("GCST1")), filters.getValue());
        assertEquals(20, page.getValue().getPageSize());

        var missing = app.get("/api/v1/graphs/g1/materialised_queries/nope");
        assertEquals(404, missing.status());
        assertEquals("Materialised query nope not found for graph g1", missing.json().getAsJsonObject().get("error").getAsString());
    }
}
