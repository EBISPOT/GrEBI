


package uk.ac.ebi.grebi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.internal.LinkedTreeMap;
import io.javalin.Javalin;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

import io.javalin.plugin.bundled.CorsPluginConfig;
import io.modelcontextprotocol.server.McpAsyncServer;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.transport.HttpServletSseServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema.ServerCapabilities;

import org.apache.solr.client.solrj.SolrQuery;
import org.eclipse.jetty.servlet.ServletHolder;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import uk.ac.ebi.grebi.repo.GrebiCypherRepo;
import uk.ac.ebi.grebi.repo.GrebiQueryTemplatesRepo;
import uk.ac.ebi.grebi.db.GrebiSolrQuery;
import uk.ac.ebi.grebi.db.ResolverClient;
import uk.ac.ebi.grebi.db.MetadataClient;
import uk.ac.ebi.grebi.db.PrefixClient;
import uk.ac.ebi.grebi.db.EmbeddingServiceClient;
import uk.ac.ebi.grebi.repo.GrebiSolrRepo;
import uk.ac.ebi.grebi.repo.GrebiPostgresRepo;
import uk.ac.ebi.grebi.repo.GrebiMetadataRepo;


public class GrebiApi {

    public static void main(String[] args) throws ParseException, org.apache.commons.cli.ParseException, IOException {

        GrebiCypherRepo cypher = null;
        GrebiSolrRepo solr = null;
        GrebiPostgresRepo postgres = null;
        GrebiMetadataRepo metadata= null;
        GrebiQueryTemplatesRepo queryTemplates = new GrebiQueryTemplatesRepo();

        Set<String> sqliteSubgraphs = null;
        Set<String> solrSubgraphs = null;
        Set<String> postgresSubgraphs = null;
        Set<String> metadataServiceSubgraphs = null;
        Set<String> cypherSubgraphs = null;

        while(true) {
            try {
                solr = new GrebiSolrRepo();
                postgres = new GrebiPostgresRepo();
                metadata = new GrebiMetadataRepo();
                sqliteSubgraphs = (new ResolverClient()).getSubgraphs();
                solrSubgraphs = solr.getSubgraphs();
                postgresSubgraphs = postgres.getSubgraphs();
                metadataServiceSubgraphs = metadata.getSubgraphs();
                if(!sqliteSubgraphs.equals(solrSubgraphs) || !sqliteSubgraphs.equals(postgresSubgraphs) || !sqliteSubgraphs.equals(metadataServiceSubgraphs)) {
                    throw new RuntimeException("SQLite/Solr/PostgreSQL/the metadata jsons do not seem to contain the same subgraphs. Found: "
                            + String.join(",", sqliteSubgraphs) + " for SQLite (from resolver service) and "
                            + String.join(",", solrSubgraphs) + " for Solr (from list of solr cores) and "
                            + String.join(",", postgresSubgraphs) + " for PostgreSQL (from edge tables) and "
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
                cypher = new GrebiCypherRepo();
                cypherSubgraphs = cypher.getSubgraphs();
                if(!sqliteSubgraphs.equals(cypherSubgraphs)) {
                    cypher = null;
                    throw new RuntimeException("SQLite/Solr/PostgreSQL/the summary jsons/cypher service do not seem to contain the same subgraphs. Found: "
                            + String.join(",", sqliteSubgraphs) + " for SQLite (from resolver service) and "
                            + String.join(",", solrSubgraphs) + " for Solr (from list of solr cores) and "
                            + String.join(",", postgresSubgraphs) + " for PostgreSQL (from edge tables) and "
                            + String.join(",", metadataServiceSubgraphs) + " for the summary jsons (from summary server) and "
                            + String.join(",", cypherSubgraphs) + " for cypher service"
                    );
                }
            } catch (Throwable e) {
                System.out.println("Could not get subgraphs from cypher service. Retrying in 10 seconds ("+ (4-i) + " attempts left)");
                e.printStackTrace();
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
            }
        }

        if(cypher == null) {
            System.out.println("Cypher service is unavailable; some graph query API endpoints will be disabled");
        } else {
            System.out.println("Cypher service is available");
        }

        System.out.println("Found subgraphs: " + String.join(",", solrSubgraphs));

        // Initialize embedding service clients (one per subgraph with PCA models)
        Map<String, EmbeddingServiceClient> embeddingClients = new LinkedHashMap<>();
        for (String sg : solrSubgraphs) {
            var meta = metadata.getMetadata(sg);
            if (meta != null && meta.containsKey("embedding_pca_models")) {
                embeddingClients.put(sg, new EmbeddingServiceClient(meta));
                System.out.println("Initialized embedding client for subgraph: " + sg);
            }
        }

        run(cypher, solr, postgres, metadata, solrSubgraphs, queryTemplates, embeddingClients);
    }

    static void run(
        final GrebiCypherRepo cypher,
        final GrebiSolrRepo solr,
        final GrebiPostgresRepo postgres,
        final GrebiMetadataRepo metadata,
        final Set<String> subgraphs,
        final GrebiQueryTemplatesRepo queryTemplates,
        final Map<String, EmbeddingServiceClient> embeddingClients
    ) {
        var stats = cypher != null ? cypher.getStats() : null;

        Gson gson = new Gson();

        GrebiMcpServer mcpServer = new GrebiMcpServer(
            cypher, solr, metadata, subgraphs, queryTemplates
        );

        Javalin.create(config -> {
              config.jetty.modifyServer(server -> {
                        var gzip = new org.eclipse.jetty.server.handler.gzip.GzipHandler();
                        gzip.addExcludedMimeTypes("text/event-stream");
                        gzip.setInflateBufferSize(0); // disable request body inflation to avoid consuming POST bodies
                        server.insertHandler(gzip);
                    });
                    config.jetty.modifyServletContextHandler(ctx -> {
                        var holder = new ServletHolder(mcpServer.getTransportProvider());
                        holder.setAsyncSupported(true);
                        ctx.addServlet(holder, "/api/v1/mcp");
                    });

                    config.bundledPlugins.enableCors(cors -> {
                        cors.addRule(CorsPluginConfig.CorsRule::anyHost);
                    });
                    config.router.contextPath = System.getenv("GREBI_CONTEXT_PATH");
                    if(config.router.contextPath == null) {
                        config.router.contextPath = "";
                    }
                })
                .get("/api/health", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result("{\"status\":\"ok\"}");
                })
                .get("/api/v1/stats", ctx -> {
                    ctx.contentType("application/json");
                    if(stats != null) {
                        ctx.result(gson.toJson(stats));
                    } else {
                        ctx.result("{\"error\":\"cypher service is not available\"}");
                    }
                })
                .get("/api/v1/topics", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(queryTemplates.getQueryTopics()));
                })
                .get("/api/v1/subgraphs", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(subgraphs));
                })
                .get("/api/v1/subgraphs/{subgraph}", ctx -> {
                    ctx.contentType("application/json");
                    var meta = new java.util.LinkedHashMap<>(metadata.getMetadata(ctx.pathParam("subgraph")));
                    meta.remove("embedding_pca_models");
                    ctx.result(gson.toJson(meta));
                })
                .get("/api/v1/subgraphs/{subgraph}/stats", ctx -> {
                    var subgraph = ctx.pathParam("subgraph");
                    ctx.contentType("application/json");

                    // Node counts by datasource and type from Solr faceting
                    var q = new GrebiSolrQuery();
                    q.addFacetField("grebi:datasources");
                    q.addFacetField("grebi:type");
                    q.setFacetLimit(-1);
                    var facetResult = solr.searchNodesPaginated(subgraph, q, false,
                            PageRequest.of(0, 1));

                    var nodeDsByDs = facetResult.facetFieldToCounts.getOrDefault("grebi:datasources", Map.of());
                    var nodesByType = facetResult.facetFieldToCounts.getOrDefault("grebi:type", Map.of());

                    // Edge counts by type from metadata edges nested structure (srcType → edgeType → dstType → dsSig → count)
                    Map<String, Long> edgesByType = new LinkedHashMap<>();
                    var meta = metadata.getMetadata(subgraph);
                    var edgesEl = meta.get("edges");
                    if (edgesEl != null && edgesEl.isJsonObject()) {
                        for (var srcType : edgesEl.getAsJsonObject().entrySet()) {
                            if (!srcType.getValue().isJsonObject()) continue;
                            for (var edgeType : srcType.getValue().getAsJsonObject().entrySet()) {
                                if (!edgeType.getValue().isJsonObject()) continue;
                                long edgeTypeTotal = 0;
                                for (var dstType : edgeType.getValue().getAsJsonObject().entrySet()) {
                                    if (!dstType.getValue().isJsonObject()) continue;
                                    for (var dsSig : dstType.getValue().getAsJsonObject().entrySet()) {
                                        edgeTypeTotal += dsSig.getValue().getAsLong();
                                    }
                                }
                                edgesByType.merge(edgeType.getKey(), edgeTypeTotal, Long::sum);
                            }
                        }
                    }

                    // Edge counts by datasource from metadata edges nested structure
                    Map<String, Long> edgeDsByDs = new LinkedHashMap<>();
                    if (edgesEl != null && edgesEl.isJsonObject()) {
                        for (var srcType : edgesEl.getAsJsonObject().entrySet()) {
                            if (!srcType.getValue().isJsonObject()) continue;
                            for (var edgeType : srcType.getValue().getAsJsonObject().entrySet()) {
                                if (!edgeType.getValue().isJsonObject()) continue;
                                for (var dstType : edgeType.getValue().getAsJsonObject().entrySet()) {
                                    if (!dstType.getValue().isJsonObject()) continue;
                                    for (var dsSig : dstType.getValue().getAsJsonObject().entrySet()) {
                                        long count = dsSig.getValue().getAsLong();
                                        for (String ds : dsSig.getKey().split(",")) {
                                            edgeDsByDs.merge(ds, count, Long::sum);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    var result = new LinkedHashMap<String, Object>();
                    result.put("node_counts_by_datasource", nodeDsByDs);
                    result.put("node_counts_by_type", nodesByType);
                    result.put("edge_counts_by_datasource", edgeDsByDs);
                    result.put("edge_counts_by_type", edgesByType);
                    ctx.result(gson.toJson(result));
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
                    ctx.header("cache-control", "no-cache");
                    ctx.result(gson.toJson(queryTemplates.getQueryTemplates().stream()
                            .filter(qt -> qt.subgraphs == null || qt.subgraphs.contains(subgraph))
                            .collect(Collectors.toList())));
                })
                .get("/api/v1/subgraphs/{subgraph}/query_templates/{templateId}", ctx -> {
                    var subgraph = ctx.pathParam("subgraph");
                    var templateId = ctx.pathParam("templateId");
                    var template = queryTemplates.getQueryTemplates().stream()
                            .filter(qt -> qt.id.equals(templateId) && (qt.subgraphs == null || qt.subgraphs.contains(subgraph)))
                            .findFirst()
                            .orElseThrow(() -> new RuntimeException("Query template " + templateId + " not found for subgraph " + subgraph));
                    ctx.contentType("application/json");
                    ctx.header("cache-control", "no-cache");
                    ctx.result(gson.toJson(template));
                })
                .get("/api/v1/subgraphs/{subgraph}/query/{templateId}.csv", ctx -> {
                    var subgraph = ctx.pathParam("subgraph");
                    var templateId = ctx.pathParam("templateId");
                    var template = queryTemplates.getQueryTemplates().stream()
                            .filter(qt -> qt.id.equals(templateId))
                            .findFirst()
                            .orElseThrow(() -> new RuntimeException("Query template " + templateId + " not found"));
                    var sortBy = Objects.requireNonNullElse(ctx.queryParam("sortBy"), template.result_columns.get(0).column_id);
                    var sortDir = Objects.requireNonNullElse(ctx.queryParam("sortDir"), "asc");

                    var params = new HashMap<String, List<String>>();
                    for (var param : ctx.queryParamMap().entrySet()) {
                        if (param.getKey().equals("page") || param.getKey().equals("size") ||
                                param.getKey().equals("templateId") || param.getKey().equals("subgraph") ||
                                param.getKey().equals("sortBy") || param.getKey().equals("sortDir") ||
                                param.getKey().equals("resolve")) {
                            continue;
                        }
                        params.put(param.getKey(), param.getValue());
                    }

                    var sort = Sort.by(sortDir.equals("asc") ? Sort.Direction.ASC : Sort.Direction.DESC, sortBy);

                    ctx.future(() -> {
                        try {
                            return cypher.runQueryFromTemplateStreamed(subgraph, template, params, sort, ctx.res());
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to write CSV response", e);
                        }
                    });
                })
                .get("/api/v1/subgraphs/{subgraph}/query/{templateId}", ctx -> {
                    var subgraph = ctx.pathParam("subgraph");
                    var templateId = ctx.pathParam("templateId");
                    var template = queryTemplates.getQueryTemplates().stream()
                            .filter(qt -> qt.id.equals(templateId))
                            .findFirst()
                            .orElseThrow(() -> new RuntimeException("Query template " + templateId + " not found"));
                    var sortBy = Objects.requireNonNullElse(ctx.queryParam("sortBy"), template.result_columns.get(0).column_id);
                    var sortDir = Objects.requireNonNullElse(ctx.queryParam("sortDir"), "asc");
                    var page_num = Objects.requireNonNullElse(ctx.queryParam("page"), "0");
                    var size = Objects.requireNonNullElse(ctx.queryParam("size"), "10");
                    var page = PageRequest.of(Integer.parseInt(page_num), Integer.parseInt(size),
                            Sort.by(sortDir.equals("asc") ? Sort.Direction.ASC : Sort.Direction.DESC, sortBy));

                    ctx.contentType("application/json");

                    var params = new HashMap<String, List<String>>();
                    for (var param : ctx.queryParamMap().entrySet()) {
                        if (param.getKey().equals("page") || param.getKey().equals("size") ||
                                param.getKey().equals("templateId") || param.getKey().equals("subgraph") ||
                                param.getKey().equals("sortBy") || param.getKey().equals("sortDir") ||
                                param.getKey().equals("resolve")) {
                            continue;
                        }
                        params.put(param.getKey(), param.getValue());
                    }

                    var resolve = "true".equals(ctx.queryParam("resolve"));

                    var res = cypher.runQueryFromTemplatePaginated(subgraph, template, params, resolve, page);

                    ctx.result(
                        gson.toJson(
                            res
                        )
                    );
                })
                .get("/api/v1/subgraphs/{subgraph}/nodes", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result("{}");

                    var q = new GrebiSolrQuery();
                    for(var param : ctx.queryParamMap().entrySet()) {
                        if(param.getKey().equals("q") ||
                                param.getKey().equals("page") ||
                                param.getKey().equals("size") ||
                                param.getKey().equals("exactMatch") ||
                                param.getKey().equals("includeObsoleteEntries") ||
                                param.getKey().equals("resolve") ||
                                param.getKey().equals("lang") ||
                                    param.getKey().equals("facet")
                        ) {
                            continue;
                        }
                        q.addFilter(param.getKey(), param.getValue(), SearchType.WHOLE_FIELD, false);
                    }

                    var res = solr.searchNodesPaginated(
                        ctx.pathParam("subgraph"),
                        q,
                        ! "false".equals(ctx.queryParam("resolve")),
                        PageRequest.of(
                            Integer.parseInt(Objects.requireNonNullElse(ctx.queryParam("page"), "0")),
                            Integer.parseInt(Objects.requireNonNullElse(ctx.queryParam("size"), "10"))
                        )
                    );

                    System.out.println("solr response: " + res.toString());

                    ctx.contentType("application/json");
                    ctx.json(res);
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
                .get("/api/v1/subgraphs/{subgraph}/nodes/{nodeId}/edge_counts", ctx -> {
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(postgres.getBothEdgeCounts(ctx.pathParam("subgraph"), nodeId)));
                })
                .get("/api/v1/subgraphs/{subgraph}/nodes/{nodeId}/incoming_edge_counts", ctx -> {
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(postgres.getIncomingEdgeCounts(ctx.pathParam("subgraph"), nodeId)));
                })
                .get("/api/v1/subgraphs/{subgraph}/nodes/{nodeId}/outgoing_edge_counts", ctx -> {
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(postgres.getOutgoingEdgeCounts(ctx.pathParam("subgraph"), nodeId)));
                })
                .post("/api/v1/subgraphs/{subgraph}/nodes/{nodeId}/resolve_single_edges", ctx -> {
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    ctx.contentType("application/json");
                    if (cypher == null) {
                        ctx.result("{}");
                        return;
                    }
                    var body = ctx.body();
                    var items = gson.fromJson(body, GrebiCypherRepo.DirectionAndEdgeType[].class);
                    if (items == null || items.length == 0) {
                        ctx.result("{}");
                        return;
                    }
                    var result = cypher.resolveSingleEdges(ctx.pathParam("subgraph"), nodeId, List.of(items));
                    ctx.result(gson.toJson(result));
                })
                .get("/api/v1/subgraphs/{subgraph}/nodes/{nodeId}/incoming_edges", ctx -> {
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    var page_num = Objects.requireNonNullElse(ctx.queryParam("page"), "0");
                    var size = Objects.requireNonNullElse(ctx.queryParam("size"), "10");
                    var sortBy = Objects.requireNonNullElse(ctx.queryParam("sortBy"), "grebi:type");
                    var sortDir = Objects.requireNonNullElse(ctx.queryParam("sortDir"), "asc");
                    var page = PageRequest.of(Integer.parseInt(page_num), Integer.parseInt(size),
                            Sort.by(sortDir.equals("asc") ? Sort.Direction.ASC : Sort.Direction.DESC, sortBy));

                    Map<String, List<String>> extraFilters = new LinkedHashMap<>();
                    for(var queryParam : ctx.queryParamMap().entrySet()) {
                        var queryParamName = queryParam.getKey();
                        if(queryParamName.equals("page") || queryParamName.equals("size")
                                || queryParamName.equals("sortBy") || queryParamName.equals("sortDir")
                                || queryParamName.equals("facet")
                        ) {
                            continue;
                        }
                        extraFilters.put(queryParamName, queryParam.getValue());
                    }

                   var res = postgres.searchEdgesPaginated(ctx.pathParam("subgraph"),
                           "grebi:toNodeId", nodeId, extraFilters, sortBy, sortDir, page);
                   ctx.contentType("application/json");
                   ctx.result(gson.toJson(res));
                })
                .get("/api/v1/subgraphs/{subgraph}/nodes/{nodeId}/outgoing_edges", ctx -> {
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    var page_num = Objects.requireNonNullElse(ctx.queryParam("page"), "0");
                    var size = Objects.requireNonNullElse(ctx.queryParam("size"), "10");
                    var sortBy = Objects.requireNonNullElse(ctx.queryParam("sortBy"), "grebi:type");
                    var sortDir = Objects.requireNonNullElse(ctx.queryParam("sortDir"), "asc");
                    var page = PageRequest.of(Integer.parseInt(page_num), Integer.parseInt(size),
                            Sort.by(sortDir.equals("asc") ? Sort.Direction.ASC : Sort.Direction.DESC, sortBy));

                    Map<String, List<String>> extraFilters = new LinkedHashMap<>();
                    for(var queryParam : ctx.queryParamMap().entrySet()) {
                        var queryParamName = queryParam.getKey();
                        if(queryParamName.equals("page") || queryParamName.equals("size")
                                || queryParamName.equals("sortBy") || queryParamName.equals("sortDir")
                                || queryParamName.equals("facet")
                        ) {
                            continue;
                        }
                        extraFilters.put(queryParamName, queryParam.getValue());
                    }

                    var res = postgres.searchEdgesPaginated(ctx.pathParam("subgraph"),
                            "grebi:fromNodeId", nodeId, extraFilters, sortBy, sortDir, page);
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res));
                })
                .get("/api/v1/subgraphs/{subgraph}/nodes/{nodeId}/incoming_edge_refs", ctx -> {
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    var page_num = Objects.requireNonNullElse(ctx.queryParam("page"), "0");
                    var size = Objects.requireNonNullElse(ctx.queryParam("size"), "10");
                    var sortBy = Objects.requireNonNullElse(ctx.queryParam("sortBy"), "grebi:type");
                    var sortDir = Objects.requireNonNullElse(ctx.queryParam("sortDir"), "asc");
                    var page = PageRequest.of(Integer.parseInt(page_num), Integer.parseInt(size),
                            Sort.by(sortDir.equals("asc") ? Sort.Direction.ASC : Sort.Direction.DESC, sortBy));

                    Map<String, List<String>> extraFilters = new LinkedHashMap<>();
                    for(var queryParam : ctx.queryParamMap().entrySet()) {
                        var queryParamName = queryParam.getKey();
                        if(queryParamName.equals("page") || queryParamName.equals("size")
                                || queryParamName.equals("sortBy") || queryParamName.equals("sortDir")
                                || queryParamName.equals("facet")
                        ) {
                            continue;
                        }
                        extraFilters.put(queryParamName, queryParam.getValue());
                    }

                    var res = postgres.searchEdgeRefsPaginated(ctx.pathParam("subgraph"),
                            "grebi:toNodeId", nodeId, extraFilters, sortBy, sortDir, page);
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res));
                })
                .get("/api/v1/subgraphs/{subgraph}/nodes/{nodeId}/outgoing_edge_refs", ctx -> {
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    var page_num = Objects.requireNonNullElse(ctx.queryParam("page"), "0");
                    var size = Objects.requireNonNullElse(ctx.queryParam("size"), "10");
                    var sortBy = Objects.requireNonNullElse(ctx.queryParam("sortBy"), "grebi:type");
                    var sortDir = Objects.requireNonNullElse(ctx.queryParam("sortDir"), "asc");
                    var page = PageRequest.of(Integer.parseInt(page_num), Integer.parseInt(size),
                            Sort.by(sortDir.equals("asc") ? Sort.Direction.ASC : Sort.Direction.DESC, sortBy));

                    Map<String, List<String>> extraFilters = new LinkedHashMap<>();
                    for(var queryParam : ctx.queryParamMap().entrySet()) {
                        var queryParamName = queryParam.getKey();
                        if(queryParamName.equals("page") || queryParamName.equals("size")
                                || queryParamName.equals("sortBy") || queryParamName.equals("sortDir")
                                || queryParamName.equals("facet")
                        ) {
                            continue;
                        }
                        extraFilters.put(queryParamName, queryParam.getValue());
                    }

                    var res = postgres.searchEdgeRefsPaginated(ctx.pathParam("subgraph"),
                            "grebi:fromNodeId", nodeId, extraFilters, sortBy, sortDir, page);
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res));
                })
                .get("/api/v1/subgraphs/{subgraph}/embedding_models", ctx -> {
                    var subgraph = ctx.pathParam("subgraph");
                    var client = embeddingClients.get(subgraph);
                    List<String> modelNames = (client != null) ? client.getAvailableModels() : List.of();
                    Set<String> embeddable = (client != null) ? client.getEmbeddableModels() : Set.of();
                    var result = new java.util.ArrayList<Map<String, Object>>();
                    for (String m : modelNames) {
                        result.add(Map.of("model", m, "can_embed", embeddable.contains(m)));
                    }
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(result));
                })
                .get("/api/v1/subgraphs/{subgraph}/semantic_search", ctx -> {
                    var subgraph = ctx.pathParam("subgraph");
                    var q = ctx.queryParam("q");
                    var model = ctx.queryParam("model");
                    var n = Integer.parseInt(Objects.requireNonNullElse(ctx.queryParam("n"), "10"));
                    var resolve = Boolean.parseBoolean(Objects.requireNonNullElse(ctx.queryParam("resolve"), "false"));

                    if (q == null || q.isBlank()) {
                        ctx.status(400).result("{\"error\":\"q parameter is required\"}");
                        return;
                    }
                    if (model == null || model.isBlank()) {
                        ctx.status(400).result("{\"error\":\"model parameter is required\"}");
                        return;
                    }

                    var client = embeddingClients.get(subgraph);
                    if (client == null) {
                        ctx.status(400).result("{\"error\":\"No embedding models available for this subgraph\"}");
                        return;
                    }

                    float[] queryVector = client.embedText(model, q);
                    var vectorResults = postgres.searchByVector(subgraph, model, queryVector, n);

                    if (resolve) {
                        var nodeIds = vectorResults.stream().map(r -> r.nodeId).toList();
                        var resolver = new ResolverClient();
                        var resolvedMap = resolver.resolveToMap(subgraph, nodeIds);
                        var results = new java.util.ArrayList<Map<String, Object>>();
                        for (var vr : vectorResults) {
                            var resolved = resolvedMap.get(vr.nodeId);
                            if (resolved == null) {
                                resolved = new java.util.HashMap<>();
                                resolved.put("grebi:nodeId", vr.nodeId);
                                resolved.put("grebi:name", vr.name != null ? List.of(vr.name) : List.of());
                            }
                            resolved.put("grebi:searchScore", 1.0 - vr.distance);
                            results.add(resolved);
                        }
                        ctx.contentType("application/json");
                        ctx.result(gson.toJson(results));
                    } else {
                        ctx.contentType("application/json");
                        ctx.result(gson.toJson(vectorResults));
                    }
                })
                .get("/api/v1/subgraphs/{subgraph}/nodes/{nodeId}/similar", ctx -> {
                    var subgraph = ctx.pathParam("subgraph");
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    var n = Integer.parseInt(Objects.requireNonNullElse(ctx.queryParam("n"), "10"));
                    var modelParam = ctx.queryParam("model");
                    String model;
                    if (modelParam != null && !modelParam.isBlank()) {
                        model = modelParam;
                    } else {
                        // Default to first available model for this subgraph
                        var embClient = embeddingClients.get(subgraph);
                        var models = (embClient != null) ? embClient.getAvailableModels() : List.<String>of();
                        if (models.isEmpty()) {
                            ctx.status(404).result("{\"error\":\"No embedding models available for this subgraph\"}");
                            return;
                        }
                        model = models.get(0);
                    }

                    var nodeEmbedding = postgres.getNodeEmbedding(subgraph, nodeId, model);
                    if (nodeEmbedding == null) {
                        ctx.status(404).result("{\"error\":\"No embedding found for this node and model\"}");
                        return;
                    }

                    var results = postgres.searchByVector(subgraph, model, nodeEmbedding, n);
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(results));
                })
                .get("/api/v1/subgraphs/{subgraph}/edges", ctx -> {
                    var page_num = Objects.requireNonNullElse(ctx.queryParam("page"), "0");
                    var size = Objects.requireNonNullElse(ctx.queryParam("size"), "10");
                    var sortBy = Objects.requireNonNullElse(ctx.queryParam("sortBy"), "grebi:type");
                    var sortDir = Objects.requireNonNullElse(ctx.queryParam("sortDir"), "asc");
                    var page = PageRequest.of(Integer.parseInt(page_num), Integer.parseInt(size),
                            Sort.by(sortDir.equals("asc") ? Sort.Direction.ASC : Sort.Direction.DESC, sortBy));

                    Map<String, List<String>> filters = new LinkedHashMap<>();
                    for(var queryParam : ctx.queryParamMap().entrySet()) {
                        var queryParamName = queryParam.getKey();
                        if(queryParamName.equals("page") || queryParamName.equals("size")
                                || queryParamName.equals("sortBy") || queryParamName.equals("sortDir")
                        ) {
                            continue;
                        }
                        filters.put(queryParamName, queryParam.getValue());
                    }

                    var res = postgres.searchEdges(ctx.pathParam("subgraph"),
                            filters, sortBy, sortDir, page);
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res));
                })
                .get("/api/v1/subgraphs/{subgraph}/edges/{edgeId}", ctx -> {
                    var rawEdgeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("edgeId")));
                    var subgraph = ctx.pathParam("subgraph");
                    // Edge IDs from Neo4j are subgraph-prefixed, but the resolver stores them without
                    var edgeId = rawEdgeId.startsWith(subgraph + ":") ? rawEdgeId.substring(subgraph.length() + 1) : rawEdgeId;
                    var resolver = new ResolverClient();
                    var resolved = resolver.resolveToMap(subgraph, List.of(edgeId));
                    var edge = resolved.get(edgeId);
                    if (edge == null) {
                        ctx.status(404).result("{\"error\":\"Edge not found\"}");
                        return;
                    }
                    Map<String, Object> refs = (Map<String, Object>) edge.get("_refs");
                    if (refs != null) {
                        Map<String, Object> retEdge = new LinkedHashMap<>(edge);
                        var fromNodeId = (String) edge.get("grebi:fromNodeId");
                        var toNodeId = (String) edge.get("grebi:toNodeId");
                        if (fromNodeId != null && refs.containsKey(fromNodeId)) {
                            retEdge.put("from", refs.get(fromNodeId));
                        }
                        if (toNodeId != null && refs.containsKey(toNodeId)) {
                            retEdge.put("to", refs.get(toNodeId));
                        }
                        edge = retEdge;
                    }
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(edge));
                })
//                .get("/api/v1/edge_types", ctx -> {
//                    ctx.contentType("application/json");
//                    ctx.result(gson.toJson(type));
//                })
                .get("/api/v1/collections", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result("{}");
                })
                .get("/api/v1/normalise_curies", ctx -> {
                    var iris_or_curies = ctx.queryParams("iris_or_curies");
                    ctx.contentType("application/json");

                    var prefixClient = new PrefixClient();
                    var res = prefixClient.reprefix(iris_or_curies);
                    ctx.result(gson.toJson(Map.of("curies", res)));
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
                                param.getKey().equals("resolve") ||
                                param.getKey().equals("lang") ||
                                    param.getKey().equals("facet")
                        ) {
                            continue;
                        }
                        q.addFilter(param.getKey(), param.getValue(), SearchType.WHOLE_FIELD, false);
                    }
                    q.addReturnField("grebi:nodeId");
                    q.addReturnField("ols:curie");
                    q.addReturnField("grebi:datasources");
                    q.addReturnField("grebi:name");
                    q.addReturnField("grebi:type");
                    q.addReturnField("grebi:sourceIds");
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
                    var resolve = ! "false".equals(ctx.queryParam("resolve"));
                    var page = PageRequest.of(Integer.parseInt(page_num), Integer.parseInt(size));
                    var res = solr.searchNodesPaginated(ctx.pathParam("subgraph"), q, resolve, page);
                    ctx.contentType("application/json");
                    ctx.json(res);
                })
                .get("/api/v1/subgraphs/{subgraph}/suggest", ctx -> {
                    var res = solr.autocomplete(ctx.pathParam("subgraph"), ctx.queryParam("q"));
                    ctx.contentType("application/json");
                    ctx.json(res);
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

