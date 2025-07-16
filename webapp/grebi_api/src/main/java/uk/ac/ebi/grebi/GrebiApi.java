


package uk.ac.ebi.grebi;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.internal.LinkedTreeMap;
import io.javalin.Javalin;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

import io.javalin.plugin.bundled.CorsPluginConfig;
import org.apache.solr.client.solrj.SolrQuery;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import uk.ac.ebi.grebi.repo.GrebiNeoRepo;
import uk.ac.ebi.grebi.repo.GrebiQueryTemplatesRepo;
import uk.ac.ebi.grebi.db.GrebiSolrQuery;
import uk.ac.ebi.grebi.db.ResolverClient;
import uk.ac.ebi.grebi.db.MetadataClient;
import uk.ac.ebi.grebi.repo.GrebiSolrRepo;
import uk.ac.ebi.grebi.repo.GrebiMetadataRepo;


public class GrebiApi {

    public static void main(String[] args) throws ParseException, org.apache.commons.cli.ParseException, IOException {

        GrebiNeoRepo neo = null;
        GrebiSolrRepo solr = null;
        GrebiMetadataRepo metadata= null;
        GrebiQueryTemplatesRepo queryTemplates = new GrebiQueryTemplatesRepo();

        Set<String> sqliteSubgraphs = null;
        Set<String> solrSubgraphs = null;
        Set<String> metadataServiceSubgraphs = null;
        Set<String> neoSubgraphs = null;

        while(true) {
            try {
                solr = new GrebiSolrRepo();
                metadata = new GrebiMetadataRepo();
                sqliteSubgraphs = (new ResolverClient()).getSubgraphs();
                solrSubgraphs = solr.getSubgraphs();
                metadataServiceSubgraphs = metadata.getSubgraphs();
                if(new HashSet<>(List.of(sqliteSubgraphs, solrSubgraphs, metadataServiceSubgraphs)).size() != 1) {
                    throw new RuntimeException("SQLite/Solr/the metadata jsons do not seem to contain the same subgraphs. Found: "
                            + String.join(",", sqliteSubgraphs) + " for SQLite (from resolver service) and "
                            + String.join(",", solrSubgraphs) + " for Solr (from list of solr cores) and "
                            + String.join(",", metadataServiceSubgraphs) + " for the summary jsons (from metadata server)"
                    );
                }
                break;
            } catch(Throwable e) {
                System.out.println("Could not get subgraphs from one of the services. Retrying in 10 seconds...");
                e.printStackTrace();
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
            }
        }

        for(int i = 0; i < 5; ++ i) {
            try {
                neo = new GrebiNeoRepo();
                neoSubgraphs = neo.getSubgraphs();
                if(new HashSet<>(List.of(sqliteSubgraphs, solrSubgraphs, metadataServiceSubgraphs)).size() != 1) {
                    neo = null;
                    throw new RuntimeException("SQLite/Solr/the summary jsons/neo4j do not seem to contain the same subgraphs. Found: "
                            + String.join(",", sqliteSubgraphs) + " for SQLite (from resolver service) and "
                            + String.join(",", solrSubgraphs) + " for Solr (from list of solr cores) and "
                            + String.join(",", metadataServiceSubgraphs) + " for the summary jsons (from summary server) and "
                            + String.join(",", neoSubgraphs) + " for neo4j"
                    );
                }
            } catch (Throwable e) {
                System.out.println("Could not get subgraphs from Neo4j. Retrying in 10 seconds ("+ (4-i) + " attempts left)");
                e.printStackTrace();
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
            }
        }

        if(neo == null) {
            System.out.println("Neo4j is unavailable; some graph query API endpoints will be disabled");
        } else {
            System.out.println("Neo4j is available");
        }

        System.out.println("Found subgraphs: " + String.join(",", solrSubgraphs));

        run(neo, solr, metadata, solrSubgraphs, queryTemplates);
    }

    static void run(
        final GrebiNeoRepo neo,
        final GrebiSolrRepo solr,
        final GrebiMetadataRepo metadata,
        final Set<String> subgraphs,
        final GrebiQueryTemplatesRepo queryTemplates
    ) {

        var stats = neo != null ? neo.getStats() : null;

        Gson gson = new Gson();

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
                    if(stats != null) {
                        ctx.result(gson.toJson(stats));
                    } else {
                        ctx.result("{\"error\":\"neo4j is not available\"}");
                    }
                })
                .get("/api/v1/topics", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(queryTemplates.queryTopics));
                })
                .get("/api/v1/subgraphs", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(subgraphs));
                })
                .get("/api/v1/subgraphs/{subgraph}", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(metadata.getMetadata(ctx.pathParam("subgraph"))));
                })
                .get("/api/v1/materialised_queries", ctx -> {
                    List<JsonElement> all_matqs = new ArrayList<>();
                    for(String graph : metadata.getSubgraphs()) {
                        var matqs = metadata.getMetadata(graph).get("materialised_queries").getAsJsonArray().asList();
                        for(var mq : matqs) {
                            // temp hack for botched dataload
                            if(mq.isJsonArray()) {
                                for(var qr : mq.getAsJsonArray()) {
                                    qr.getAsJsonObject().addProperty("subgraph", graph);
                                    all_matqs.add(qr);
                                }
                            } else {
                                mq.getAsJsonObject().addProperty("subgraph", graph);
                                all_matqs.add(mq);

                            }
                        }
                    }
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(all_matqs));
                })
                .get("/api/v1/subgraphs/{subgraph}/materialised_queries", ctx -> {
                    var md = metadata.getMetadata(ctx.pathParam("subgraph"));
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(md.get("materialised_queries")));
                })
                .get("/api/v1/subgraphs/{subgraph}/materialised_queries/{queryid}", ctx -> {
                    var q = new GrebiSolrQuery();
                    q.setSearchText(ctx.queryParam("q"));
                    q.setExactMatch(false);
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
                    var res = solr.searchResultsPaginated(ctx.pathParam("subgraph"), ctx.pathParam("queryid"), q, page);
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res));
                })
                .get("/api/v1/subgraphs/{subgraph}/query_templates", ctx -> {
                    var subgraph = ctx.pathParam("subgraph");
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(queryTemplates.queryTemplates.stream()
                            .filter(qt -> qt.subgraphs == null || qt.subgraphs.contains(subgraph))
                            .collect(Collectors.toList())));
                })
                .get("/api/v1/subgraphs/{subgraph}/query/{templateId}", ctx -> {
                    var subgraph = ctx.pathParam("subgraph");
                    var templateId = ctx.pathParam("templateId");
                    var template = queryTemplates.queryTemplates.stream()
                            .filter(qt -> qt.id.equals(templateId))
                            .findFirst()
                            .orElseThrow(() -> new RuntimeException("Query template " + templateId + " not found"));

                    ctx.contentType("application/json");

                    var page_num = ctx.queryParam("page");
                    if(page_num == null) {
                        page_num = "0";
                    }
                    var size = ctx.queryParam("size");
                    if(size == null) {
                        size = "10";
                    }
                    var page = PageRequest.of(Integer.parseInt(page_num), Integer.parseInt(size));
                    var res = neo.runQueryFromTemplate(subgraph, template, ctx.queryParamMap(), page);

                    ctx.result(
                        gson.toJson(
                            res
                        )
                    );
                })
                .get("/api/v1/subgraphs/{subgraph}/nodes/{nodeId}", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result("{}");

                    String nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));

                    var q = new GrebiSolrQuery();
                    q.addFilter("grebi:nodeId", List.of(nodeId), SearchType.WHOLE_FIELD, false);

                    var res = solr.getFirstNode(ctx.pathParam("subgraph"), q);

                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res));
                })
                .get("/api/v1/subgraphs/{subgraph}/nodes/{nodeId}/incoming_edge_counts", ctx -> {
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(solr.getIncomingEdgeCounts(ctx.pathParam("subgraph"), nodeId)));
                })
                .get("/api/v1/subgraphs/{subgraph}/nodes/{nodeId}/outgoing_edge_counts", ctx -> {
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(solr.getIncomingEdgeCounts(ctx.pathParam("subgraph"), nodeId)));
                })
                .get("/api/v1/subgraphs/{subgraph}/nodes/{nodeId}/incoming_edges", ctx -> {
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    var page_num = Objects.requireNonNullElse(ctx.queryParam("page"), "0");
                    var size = Objects.requireNonNullElse(ctx.queryParam("size"), "10");
                    var sortBy = Objects.requireNonNullElse(ctx.queryParam("sortBy"), "grebi:type");
                    var sortDir = Objects.requireNonNullElse(ctx.queryParam("sortDir"), "asc");
                    var page = PageRequest.of(Integer.parseInt(page_num), Integer.parseInt(size),
                            Sort.by(sortDir.equals("asc") ? Sort.Direction.ASC : Sort.Direction.DESC, sortBy));

                    var q = new GrebiSolrQuery();

                    for(var facet : ctx.queryParams("facet")) {
                        q.addFacetField(facet);
                    }

                    q.addFilter("grebi:toNodeId", Set.of(nodeId),
                           /* this is actually a string field so this is an exact match */ SearchType.CASE_INSENSITIVE_TOKENS,
                           false);

                    for(var queryParam : ctx.queryParamMap().entrySet()) {
                        var queryParamName = queryParam.getKey();
                        if(queryParamName.equals("page") || queryParamName.equals("size")
                                || queryParamName.equals("sortBy") || queryParamName.equals("sortDir")
                                || queryParamName.equals("facet")
                        ) {
                            continue;
                        }
                        q.addFilter(queryParamName.replace("-", ""),
                                queryParam.getValue(), SearchType.WHOLE_FIELD, queryParamName.startsWith("-"));
                    }

                   var res = solr.searchEdgesPaginated(ctx.pathParam("subgraph"), q, page);
                   ctx.contentType("application/json");
                   ctx.result(gson.toJson(res
                           .map(edge -> {
                               Map<String, Object> refs = (Map<String,Object>) edge.get("_refs");
                               Map<String, Object> retEdge = new LinkedHashMap<>(edge);
                               retEdge.put("from", refs.get((String) edge.get("grebi:fromNodeId")));
                               retEdge.put("to", refs.get((String) edge.get("grebi:toNodeId")));

//                               String type = (String)edge.get("grebi:type");
//                               if(refs.containsKey(type)) {
//                                   retEdge.put("grebi:type", refs.get(type));
//                               }

                               return retEdge;
                           }))
                   );
                })
                .get("/api/v1/subgraphs/{subgraph}/nodes/{nodeId}/outgoing_edges", ctx -> {
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    var page_num = Objects.requireNonNullElse(ctx.queryParam("page"), "0");
                    var size = Objects.requireNonNullElse(ctx.queryParam("size"), "10");
                    var sortBy = Objects.requireNonNullElse(ctx.queryParam("sortBy"), "grebi:type");
                    var sortDir = Objects.requireNonNullElse(ctx.queryParam("sortDir"), "asc");
                    var page = PageRequest.of(Integer.parseInt(page_num), Integer.parseInt(size),
                            Sort.by(sortDir.equals("asc") ? Sort.Direction.ASC : Sort.Direction.DESC, sortBy));

                    var q = new GrebiSolrQuery();

                    for(var facet : ctx.queryParams("facet")) {
                        q.addFacetField(facet);
                    }

                    q.addFilter("grebi:fromNodeId", Set.of(nodeId),
                            /* this is actually a string field so this is an exact match */ SearchType.CASE_INSENSITIVE_TOKENS,
                            false);

                    for(var queryParam : ctx.queryParamMap().entrySet()) {
                        var queryParamName = queryParam.getKey();
                        if(queryParamName.equals("page") || queryParamName.equals("size")
                                || queryParamName.equals("sortBy") || queryParamName.equals("sortDir")
                                || queryParamName.equals("facet")
                        ) {
                            continue;
                        }
                        q.addFilter(queryParamName.replace("-", ""),
                                queryParam.getValue(), SearchType.WHOLE_FIELD, queryParamName.startsWith("-"));
                    }

                    var res = solr.searchEdgesPaginated(ctx.pathParam("subgraph"), q, page);
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res
                                    .map(edge -> {
                                        Map<String, Object> refs = (Map<String,Object>) edge.get("_refs");
                                        Map<String, Object> retEdge = new LinkedHashMap<>(edge);
                                        retEdge.put("from", refs.get((String) edge.get("grebi:fromNodeId")));
                                        retEdge.put("to", refs.get((String) edge.get("grebi:toNodeId")));

//                               String type = (String)edge.get("grebi:type");
//                               if(refs.containsKey(type)) {
//                                   retEdge.put("grebi:type", refs.get(type));
//                               }

                                        return retEdge;
                                    }))
                    );
                })
//                .get("/api/v1/edge_types", ctx -> {
//                    ctx.contentType("application/json");
//                    ctx.result(gson.toJson(type));
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
                .exception(Exception.class, (e, ctx) -> {
                    ctx.status(500);
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(Map.of("error", e.getMessage())));
                    e.printStackTrace();
                })
                .start("0.0.0.0", 8090);
    }

}

