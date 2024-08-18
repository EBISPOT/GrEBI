


package uk.ac.ebi.grebi;

import com.google.gson.Gson;
import io.javalin.Javalin;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

import io.javalin.plugin.bundled.CorsPluginConfig;
import org.springframework.data.domain.PageRequest;
import uk.ac.ebi.grebi.repo.GrebiNeoRepo;
import uk.ac.ebi.grebi.db.GrebiSolrQuery;
import uk.ac.ebi.grebi.db.ResolverClient;
import uk.ac.ebi.grebi.db.SummaryClient;
import uk.ac.ebi.grebi.repo.GrebiSolrRepo;
import uk.ac.ebi.grebi.repo.GrebiSummaryRepo;


public class GrebiApi {

    public static void main(String[] args) throws ParseException, org.apache.commons.cli.ParseException, IOException {

        final GrebiNeoRepo neo = new GrebiNeoRepo();
        final GrebiSolrRepo solr = new GrebiSolrRepo();
        final GrebiSummaryRepo summary = new GrebiSummaryRepo();

        Gson gson = new Gson();

        var stats = neo.getStats();

        var rocksDbSubgraphs = (new ResolverClient()).getSubgraphs();
        var solrSubgraphs = solr.getSubgraphs();
        var summarySubgraphs = summary.getSubgraphs();

        if(new HashSet<>(List.of(rocksDbSubgraphs, solrSubgraphs, summarySubgraphs)).size() != 1) {
            throw new RuntimeException("RocksDB/Solr/the summary jsons do not seem to contain the same subgraphs. Found: " + String.join(",", rocksDbSubgraphs) + " for RocksDB (from resolver service) and " + String.join(",", solrSubgraphs) + " for Solr (from list of solr cores) and " + String.join(",", summarySubgraphs) + " for the summary jsons (from summary server)");
        }

        System.out.println("Found subgraphs: " + String.join(",", solrSubgraphs));

        Javalin.create(config -> {
                    config.bundledPlugins.enableCors(cors -> {
                        cors.addRule(CorsPluginConfig.CorsRule::anyHost);
                    });
                    config.router.contextPath = System.getenv("GREBI_CONTEXT_PATH");
                    if(config.router.contextPath == null) {
                        config.router.contextPath = "";
                    }
                })
                .get("/api/v1/stats", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(stats));
                })
                .get("/api/v1/subgraphs", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(solrSubgraphs));
                })
                .get("/api/v1/subgraphs/{subgraph}", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(summary.getSummary(ctx.pathParam("subgraph"))));
                })
                .get("/api/v1/subgraphs/{subgraph}/nodes/{nodeId}", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result("{}");

                    var q = new GrebiSolrQuery();
                    q.addFilter("grebi:nodeId", List.of(ctx.pathParam("nodeId")), SearchType.WHOLE_FIELD, false);

                    var res = solr.getFirstNode(ctx.pathParam("subgraph"), q);

                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res));
                })
                .get("/api/v1/subgraphs/{subgraph}/nodes/{nodeId}/incoming_edges", ctx -> {
                   ctx.contentType("application/json");
                   ctx.result(gson.toJson(neo.getIncomingEdges(ctx.pathParam("subgraph"), ctx.pathParam("nodeId"))));
                })
//                .get("/api/v1/edge_types", ctx -> {
//                    ctx.contentType("application/json");
//                    ctx.result(gson.toJson(edgeTypes));
//                })
                .get("/api/v1/collections", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result("{}");
                })
                .get("/api/v1/subgraphs/{subgraph}/search", ctx -> {
                    var q = new GrebiSolrQuery();
                    q.setSearchText(ctx.queryParam("q"));
                    q.setExactMatch(false);
                    q.addSearchField("id", 1000, SearchType.WHOLE_FIELD);
                    q.addSearchField("grebi:name", 900, SearchType.WHOLE_FIELD);
                    q.addSearchField("grebi:synonym", 800, SearchType.WHOLE_FIELD);
                    q.addSearchField("id", 500, SearchType.CASE_INSENSITIVE_TOKENS);
                    q.addSearchField("grebi:name", 450, SearchType.CASE_INSENSITIVE_TOKENS);
                    q.addSearchField("grebi:synonym", 420, SearchType.CASE_INSENSITIVE_TOKENS);
                    q.addSearchField("grebi:description", 400, SearchType.WHOLE_FIELD);
                    q.addSearchField("grebi:description", 250, SearchType.CASE_INSENSITIVE_TOKENS);
                    q.addSearchField("_text_", 1, SearchType.CASE_INSENSITIVE_TOKENS);
                    q.addFilter("ols:isObsolete", Set.of("true"), SearchType.WHOLE_FIELD, true);
                    for(var param : ctx.queryParamMap().entrySet()) {
                        if(param.getKey().equals("q") ||
                                param.getKey().equals("page") ||
                                param.getKey().equals("size") ||
                                param.getKey().equals("exactMatch") ||
                                param.getKey().equals("includeObsoleteEntries") ||
                                param.getKey().equals("lang") ||
                                    param.getKey().equals("facet")
                        ) {
                            continue;
                        }
                        q.addFilter(param.getKey(), param.getValue(), SearchType.WHOLE_FIELD, false);
                    }
                    for(var facetField : ctx.queryParams("facet")) {
                        q.addFacetField(facetField);
                    }
                    var page_num = ctx.queryParam("page");
                    if(page_num == null) {
                        page_num = "0";
                    }
                    var size = ctx.queryParam("size");
                    if(size == null) {
                        size = "10";
                    }
                    var page = PageRequest.of(Integer.parseInt(page_num), Integer.parseInt(size));
                    var res = solr.searchNodesPaginated(ctx.pathParam("subgraph"), q, page);
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res));
                })
                .get("/api/v1/subgraphs/{subgraph}/suggest", ctx -> {
                    var res = solr.autocomplete(ctx.pathParam("subgraph"), ctx.queryParam("q"));
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res));
                })
                .start("0.0.0.0", 8090);
    }

}

