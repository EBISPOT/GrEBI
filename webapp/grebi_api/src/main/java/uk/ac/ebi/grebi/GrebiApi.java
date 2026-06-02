


package uk.ac.ebi.grebi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.internal.LinkedTreeMap;
import io.javalin.Javalin;
import io.javalin.http.HttpResponseException;
import io.javalin.http.NotFoundResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

import io.javalin.plugin.bundled.CorsPluginConfig;
import io.modelcontextprotocol.server.McpAsyncServer;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.transport.HttpServletSseServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema.ServerCapabilities;

import org.eclipse.jetty.servlet.ServletHolder;
import org.springframework.data.domain.Sort;
import uk.ac.ebi.grebi.GraphOrder;
import uk.ac.ebi.grebi.repo.GrebiCypherRepo;
import uk.ac.ebi.grebi.repo.QueryTemplate;
import uk.ac.ebi.grebi.repo.GrebiQueryTemplatesRepo;
import uk.ac.ebi.grebi.db.PrefixClient;
import uk.ac.ebi.grebi.db.EmbeddingServiceClient;
import uk.ac.ebi.grebi.repo.GrebiPostgresRepo;
import uk.ac.ebi.grebi.repo.GrebiMetadataRepo;


public class GrebiApi {

    public static void main(String[] args) throws ParseException, org.apache.commons.cli.ParseException, IOException {

        GrebiCypherRepo cypher = null;
        GrebiPostgresRepo postgres = null;
        GrebiMetadataRepo metadata= null;
        GrebiQueryTemplatesRepo queryTemplates = new GrebiQueryTemplatesRepo();

        Set<String> postgresGraphs = null;
        Set<String> cypherGraphs = null;

        while(true) {
            try {
                postgres = new GrebiPostgresRepo();
                postgresGraphs = postgres.getGraphs();
                metadata = new GrebiMetadataRepo(postgres.getPgClient());
                break;
            } catch(Throwable e) {
                System.out.println("Could not get graphs from one of the services. Retrying in 10 seconds...");
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
                cypherGraphs = cypher.getGraphs();
                if(!postgresGraphs.equals(cypherGraphs)) {
                    cypher = null;
                    throw new RuntimeException("PostgreSQL/cypher service do not seem to contain the same graphs. Found: "
                            + String.join(",", postgresGraphs) + " for PostgreSQL and "
                            + String.join(",", cypherGraphs) + " for cypher service"
                    );
                }
            } catch (Throwable e) {
                System.out.println("Could not get graphs from cypher service. Retrying in 10 seconds ("+ (4-i) + " attempts left)");
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

        postgresGraphs = GraphOrder.orderedSet(postgresGraphs);

        System.out.println("Found graphs: " + String.join(",", postgresGraphs));
        postgres.refreshExampleEdgeCountCacheAsync(queryTemplates.getQueryTemplates());
        queryTemplates.addReloadListener(postgres::refreshExampleEdgeCountCacheAsync);

        // Initialize embedding service clients (one per graph with PCA models)
        Map<String, EmbeddingServiceClient> embeddingClients = new LinkedHashMap<>();
        for (String sg : postgresGraphs) {
            var meta = metadata.getMetadata(sg);
            if (meta != null && meta.containsKey("embedding_pca_models")) {
                embeddingClients.put(sg, new EmbeddingServiceClient(meta));
                System.out.println("Initialized embedding client for graph: " + sg);
            }
        }

        run(cypher, postgres, metadata, postgresGraphs, queryTemplates, embeddingClients);
    }

    static void run(
        final GrebiCypherRepo cypher,
        final GrebiPostgresRepo postgres,
        final GrebiMetadataRepo metadata,
        final Set<String> graphs,
        final GrebiQueryTemplatesRepo queryTemplates,
        final Map<String, EmbeddingServiceClient> embeddingClients
    ) {
        var stats = cypher != null ? cypher.getStats() : null;
        var port = Integer.parseInt(Objects.requireNonNullElse(System.getenv("GREBI_PORT"), "8090"));

        Gson gson = new Gson();
        ResourceLimits limits = ResourceLimits.get();

        GrebiMcpServer mcpServer = new GrebiMcpServer(
            cypher, postgres, metadata, graphs, queryTemplates
        );

        Javalin.create(config -> {
                    config.http.gzipOnlyCompression();
                    config.http.maxRequestSize = limits.maxRequestBodyBytes();
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
                .before(ctx -> {
                    limits.checkRateLimit("http:" + ctx.ip());
                    limits.validateQueryString(ctx.queryString());
                    limits.validateQueryParams(ctx.queryParamMap());
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
                .get("/api/v1/graphs", ctx -> {
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(graphs));
                })
                .get("/api/v1/graphs/{graph}", ctx -> {
                    ctx.contentType("application/json");
                    var meta = new java.util.LinkedHashMap<>(metadata.getMetadata(ctx.pathParam("graph")));
                    meta.remove("embedding_pca_models");
                    ctx.result(gson.toJson(meta));
                })
                .get("/api/v1/graphs/{graph}/stats", ctx -> {
                    var graph = ctx.pathParam("graph");
                    ctx.contentType("application/json");

                    var meta = metadata.getMetadata(graph);

                    // Node counts by type from precomputed metadata
                    Map<String, Long> nodesByType = new LinkedHashMap<>();
                    var typesEl = meta.get("types");
                    if (typesEl != null && typesEl.isJsonObject()) {
                        for (var entry : typesEl.getAsJsonObject().entrySet()) {
                            if (entry.getValue().isJsonObject()) {
                                var countEl = entry.getValue().getAsJsonObject().get("count");
                                if (countEl != null) {
                                    nodesByType.put(entry.getKey(), countEl.getAsLong());
                                }
                            }
                        }
                    }

                    // Node counts by datasource from precomputed metadata
                    Map<String, Long> nodeDsByDs = new LinkedHashMap<>();
                    var nodeDsEl = meta.get("node_counts_by_datasource");
                    if (nodeDsEl != null && nodeDsEl.isJsonObject()) {
                        for (var entry : nodeDsEl.getAsJsonObject().entrySet()) {
                            nodeDsByDs.put(entry.getKey(), entry.getValue().getAsLong());
                        }
                    }

                    // Edge counts by type from metadata edges nested structure (srcType → edgeType → dstType → dsSig → count)
                    Map<String, Long> edgesByType = new LinkedHashMap<>();
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
                    for(String graph : metadata.getGraphs()) {
                        var matqs = metadata.getMetadata(graph).get("materialised_queries").getAsJsonArray().asList();
                        for(var mq : matqs) {
                            // temp hack for botched dataload
                            if(mq.isJsonArray()) {
                                for(var qr : mq.getAsJsonArray()) {
                                    qr.getAsJsonObject().addProperty("graph", graph);
                                    all_matqs.add(qr);
                                }
                            } else {
                                mq.getAsJsonObject().addProperty("graph", graph);
                                all_matqs.add(mq);

                            }
                        }
                    }
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(all_matqs));
                })
                .get("/api/v1/graphs/{graph}/materialised_queries", ctx -> {
                    var md = metadata.getMetadata(ctx.pathParam("graph"));
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(md.get("materialised_queries")));
                })
                .get("/api/v1/graphs/{graph}/materialised_queries/{queryid}", ctx -> {
                    var searchText = ctx.queryParam("q");
                    limits.validateText(searchText, "q");

                    Map<String, List<String>> filters = new LinkedHashMap<>();
                    List<String> facetFields = new ArrayList<>();
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
                        filters.put(param.getKey(), param.getValue());
                    }
                    for(var facetField : ctx.queryParams("facet")) {
                        facetFields.add(facetField);
                    }
                    var page = limits.pageRequest(ctx.queryParam("page"), ctx.queryParam("size"));
                    var res = postgres.searchMaterialisedQueryResultsPaginated(
                            ctx.pathParam("graph"), ctx.pathParam("queryid"),
                            searchText, filters, facetFields, page);
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res));
                })
                .get("/api/v1/graphs/{graph}/query_templates", ctx -> {
                    var graph = ctx.pathParam("graph");
                    ctx.contentType("application/json");
                    ctx.header("cache-control", "no-cache");
                    ctx.result(gson.toJson(queryTemplates.getQueryTemplates().stream()
                            .filter(qt -> qt.graphs == null || qt.graphs.contains(graph))
                            .collect(Collectors.toList())));
                })
                .get("/api/v1/graphs/{graph}/query_templates/{templateId}", ctx -> {
                    var graph = ctx.pathParam("graph");
                    var templateId = ctx.pathParam("templateId");
                    var template = getQueryTemplateOrThrow(queryTemplates, graph, templateId);
                    ctx.contentType("application/json");
                    ctx.header("cache-control", "no-cache");
                    ctx.result(gson.toJson(template));
                })
                .get("/api/v1/graphs/{graph}/query/{templateId}.csv", ctx -> {
                    var graph = ctx.pathParam("graph");
                    var templateId = ctx.pathParam("templateId");
                    var template = getQueryTemplateOrThrow(queryTemplates, graph, templateId);
                    var sortBy = Objects.requireNonNullElse(ctx.queryParam("sortBy"), template.result_columns.get(0).column_id);
                    var sortDir = Objects.requireNonNullElse(ctx.queryParam("sortDir"), "asc");

                    var params = new HashMap<String, List<String>>();
                    for (var param : ctx.queryParamMap().entrySet()) {
                        if (param.getKey().equals("page") || param.getKey().equals("size") ||
                                param.getKey().equals("templateId") || param.getKey().equals("graph") ||
                                param.getKey().equals("sortBy") || param.getKey().equals("sortDir") ||
                                param.getKey().equals("resolve")) {
                            continue;
                        }
                        params.put(param.getKey(), param.getValue());
                    }

                    var sort = Sort.by(sortDir.equals("asc") ? Sort.Direction.ASC : Sort.Direction.DESC, sortBy);

                    ctx.future(() -> {
                        try {
                            return cypher.runQueryFromTemplateStreamed(graph, template, params, sort, ctx.res());
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to write CSV response", e);
                        }
                    });
                })
                .get("/api/v1/graphs/{graph}/query/{templateId}", ctx -> {
                    var graph = ctx.pathParam("graph");
                    var templateId = ctx.pathParam("templateId");
                    var template = getQueryTemplateOrThrow(queryTemplates, graph, templateId);
                    var sortBy = Objects.requireNonNullElse(ctx.queryParam("sortBy"), template.result_columns.get(0).column_id);
                    var sortDir = Objects.requireNonNullElse(ctx.queryParam("sortDir"), "asc");
                    var page = limits.pageRequest(ctx.queryParam("page"), ctx.queryParam("size"),
                            Sort.by(sortDir.equals("asc") ? Sort.Direction.ASC : Sort.Direction.DESC, sortBy));

                    ctx.contentType("application/json");

                    var params = new HashMap<String, List<String>>();
                    for (var param : ctx.queryParamMap().entrySet()) {
                        if (param.getKey().equals("page") || param.getKey().equals("size") ||
                                param.getKey().equals("templateId") || param.getKey().equals("graph") ||
                                param.getKey().equals("sortBy") || param.getKey().equals("sortDir") ||
                                param.getKey().equals("resolve")) {
                            continue;
                        }
                        params.put(param.getKey(), param.getValue());
                    }

                    var resolve = "true".equals(ctx.queryParam("resolve"));

                    var res = cypher.runQueryFromTemplatePaginated(graph, template, params, resolve, page);

                    ctx.result(
                        gson.toJson(
                            res
                        )
                    );
                })
                .get("/api/v1/graphs/{graph}/nodes", ctx -> {
                    ctx.contentType("application/json");

                    Map<String, List<String>> filters = new LinkedHashMap<>();
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
                        filters.put(param.getKey(), param.getValue());
                    }

                    var resolve = ! "false".equals(ctx.queryParam("resolve"));
                    var res = postgres.searchNodesPaginated(
                        ctx.pathParam("graph"),
                        null,
                        filters,
                        resolve,
                        limits.pageRequest(ctx.queryParam("page"), ctx.queryParam("size"))
                    );

                    ctx.json(res);
                })
                .get("/api/v1/graphs/{graph}/nodes/{nodeId}", ctx -> {
                    ctx.contentType("application/json");

                    String nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));

                    var pgClient = postgres.getPgClient();
                    var resolved = pgClient.resolveToList(ctx.pathParam("graph"), List.of(nodeId));
                    var res = resolved.isEmpty() ? null : resolved.get(0);

                    if (res == null) {
                        ctx.status(404).result("{\"error\":\"Node not found\"}");
                        return;
                    }
                    ctx.result(gson.toJson(res));
                })
                .get("/api/v1/graphs/{graph}/nodes/{nodeId}/edge_counts", ctx -> {
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    ctx.contentType("application/json");
                    ctx.header("cache-control", "public, max-age=600");
                    ctx.result(gson.toJson(postgres.getBothEdgeCounts(ctx.pathParam("graph"), nodeId)));
                })
                .get("/api/v1/graphs/{graph}/nodes/{nodeId}/incoming_edge_counts", ctx -> {
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    ctx.contentType("application/json");
                    ctx.header("cache-control", "public, max-age=600");
                    ctx.result(gson.toJson(postgres.getIncomingEdgeCounts(ctx.pathParam("graph"), nodeId)));
                })
                .get("/api/v1/graphs/{graph}/nodes/{nodeId}/outgoing_edge_counts", ctx -> {
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    ctx.contentType("application/json");
                    ctx.header("cache-control", "public, max-age=600");
                    ctx.result(gson.toJson(postgres.getOutgoingEdgeCounts(ctx.pathParam("graph"), nodeId)));
                })
                .post("/api/v1/graphs/{graph}/nodes/{nodeId}/resolve_single_edges", ctx -> {
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    ctx.contentType("application/json");
                    if (cypher == null) {
                        ctx.result("{}");
                        return;
                    }
                    var bodyBytes = ctx.bodyAsBytes();
                    limits.validateRequestBody(bodyBytes);
                    var body = new String(bodyBytes, StandardCharsets.UTF_8);
                    var items = gson.fromJson(body, GrebiCypherRepo.DirectionAndEdgeType[].class);
                    if (items == null || items.length == 0) {
                        ctx.result("{}");
                        return;
                    }
                    limits.validateResolveSingleEdgesCount(items.length);
                    var result = cypher.resolveSingleEdges(ctx.pathParam("graph"), nodeId, List.of(items));
                    ctx.result(gson.toJson(result));
                })
                .get("/api/v1/graphs/{graph}/nodes/{nodeId}/incoming_edges", ctx -> {
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    var sortBy = Objects.requireNonNullElse(ctx.queryParam("sortBy"), "grebi:type");
                    var sortDir = Objects.requireNonNullElse(ctx.queryParam("sortDir"), "asc");
                    var page = limits.pageRequest(ctx.queryParam("page"), ctx.queryParam("size"),
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

                   var res = postgres.searchEdgesPaginated(ctx.pathParam("graph"),
                           "grebi:toNodeId", nodeId, extraFilters, sortBy, sortDir, page);
                   ctx.contentType("application/json");
                   ctx.result(gson.toJson(res));
                })
                .get("/api/v1/graphs/{graph}/nodes/{nodeId}/outgoing_edges", ctx -> {
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    var sortBy = Objects.requireNonNullElse(ctx.queryParam("sortBy"), "grebi:type");
                    var sortDir = Objects.requireNonNullElse(ctx.queryParam("sortDir"), "asc");
                    var page = limits.pageRequest(ctx.queryParam("page"), ctx.queryParam("size"),
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

                    var res = postgres.searchEdgesPaginated(ctx.pathParam("graph"),
                            "grebi:fromNodeId", nodeId, extraFilters, sortBy, sortDir, page);
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res));
                })
                .get("/api/v1/graphs/{graph}/nodes/{nodeId}/incoming_edge_refs", ctx -> {
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    var sortBy = Objects.requireNonNullElse(ctx.queryParam("sortBy"), "grebi:type");
                    var sortDir = Objects.requireNonNullElse(ctx.queryParam("sortDir"), "asc");
                    var page = limits.pageRequest(ctx.queryParam("page"), ctx.queryParam("size"),
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

                    var res = postgres.searchEdgeRefsPaginated(ctx.pathParam("graph"),
                            "grebi:toNodeId", nodeId, extraFilters, sortBy, sortDir, page);
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res));
                })
                .get("/api/v1/graphs/{graph}/nodes/{nodeId}/outgoing_edge_refs", ctx -> {
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    var sortBy = Objects.requireNonNullElse(ctx.queryParam("sortBy"), "grebi:type");
                    var sortDir = Objects.requireNonNullElse(ctx.queryParam("sortDir"), "asc");
                    var page = limits.pageRequest(ctx.queryParam("page"), ctx.queryParam("size"),
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

                    var res = postgres.searchEdgeRefsPaginated(ctx.pathParam("graph"),
                            "grebi:fromNodeId", nodeId, extraFilters, sortBy, sortDir, page);
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res));
                })
                .get("/api/v1/graphs/{graph}/embedding_models", ctx -> {
                    var graph = ctx.pathParam("graph");
                    var client = embeddingClients.get(graph);
                    List<String> modelNames = (client != null) ? client.getAvailableModels() : List.of();
                    Set<String> embeddable = (client != null) ? client.getEmbeddableModels() : Set.of();
                    var result = new java.util.ArrayList<Map<String, Object>>();
                    for (String m : modelNames) {
                        result.add(Map.of("model", m, "can_embed", embeddable.contains(m)));
                    }
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(result));
                })
                .get("/api/v1/graphs/{graph}/semantic_search", ctx -> {
                    var graph = ctx.pathParam("graph");
                    var q = ctx.queryParam("q");
                    var model = ctx.queryParam("model");
                    var n = limits.vectorLimit(ctx.queryParam("n"));
                    var resolve = Boolean.parseBoolean(Objects.requireNonNullElse(ctx.queryParam("resolve"), "false"));

                    if (q == null || q.isBlank()) {
                        ctx.status(400).result("{\"error\":\"q parameter is required\"}");
                        return;
                    }
                    limits.validateText(q, "q");
                    if (model == null || model.isBlank()) {
                        ctx.status(400).result("{\"error\":\"model parameter is required\"}");
                        return;
                    }

                    var client = embeddingClients.get(graph);
                    if (client == null) {
                        ctx.status(400).result("{\"error\":\"No embedding models available for this graph\"}");
                        return;
                    }

                    float[] queryVector = client.embedText(model, q);
                    var vectorResults = postgres.searchByVector(graph, model, queryVector, n);

                    if (resolve) {
                        var nodeIds = vectorResults.stream().map(r -> r.nodeId).toList();
                        var pgClient = postgres.getPgClient();
                        var resolvedMap = pgClient.resolveToMap(graph, nodeIds);
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
                .get("/api/v1/graphs/{graph}/nodes/{nodeId}/similar", ctx -> {
                    var graph = ctx.pathParam("graph");
                    var nodeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("nodeId")));
                    var n = limits.vectorLimit(ctx.queryParam("n"));
                    var modelParam = ctx.queryParam("model");
                    String model;
                    if (modelParam != null && !modelParam.isBlank()) {
                        model = modelParam;
                    } else {
                        // Default to first available model for this graph
                        var embClient = embeddingClients.get(graph);
                        var models = (embClient != null) ? embClient.getAvailableModels() : List.<String>of();
                        if (models.isEmpty()) {
                            ctx.status(404).result("{\"error\":\"No embedding models available for this graph\"}");
                            return;
                        }
                        model = models.get(0);
                    }

                    var nodeEmbedding = postgres.getNodeEmbedding(graph, nodeId, model);
                    if (nodeEmbedding == null) {
                        ctx.status(404).result("{\"error\":\"No embedding found for this node and model\"}");
                        return;
                    }

                    var results = postgres.searchByVector(graph, model, nodeEmbedding, n);
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(results));
                })
                .get("/api/v1/graphs/{graph}/edges", ctx -> {
                    var sortBy = Objects.requireNonNullElse(ctx.queryParam("sortBy"), "grebi:type");
                    var sortDir = Objects.requireNonNullElse(ctx.queryParam("sortDir"), "asc");
                    var page = limits.pageRequest(ctx.queryParam("page"), ctx.queryParam("size"),
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

                    var res = postgres.searchEdges(ctx.pathParam("graph"),
                            filters, sortBy, sortDir, page);
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(res));
                })
                .get("/api/v1/graphs/{graph}/edges/{edgeId}", ctx -> {
                    var rawEdgeId = new String(Base64.getUrlDecoder().decode(ctx.pathParam("edgeId")));
                    var graph = ctx.pathParam("graph");
                    // Edge IDs from Neo4j are graph-prefixed, but the resolver stores them without
                    var edgeId = rawEdgeId.startsWith(graph + ":") ? rawEdgeId.substring(graph.length() + 1) : rawEdgeId;
                    var pgClient = postgres.getPgClient();
                    var resolved = pgClient.resolveToMap(graph, List.of(edgeId));
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
                .get("/api/v1/graphs/{graph}/search", ctx -> {
                    var searchText = ctx.queryParam("q");
                    limits.validateText(searchText, "q");

                    Map<String, List<String>> filters = new LinkedHashMap<>();
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
                        filters.put(param.getKey(), param.getValue());
                    }
                    var resolve = ! "false".equals(ctx.queryParam("resolve"));
                    var page = limits.pageRequest(ctx.queryParam("page"), ctx.queryParam("size"));
                    var res = postgres.searchNodesPaginated(ctx.pathParam("graph"), searchText, filters, resolve, page);
                    ctx.contentType("application/json");
                    ctx.json(res);
                })
                .get("/api/v1/graphs/{graph}/suggest", ctx -> {
                    limits.validateText(ctx.queryParam("q"), "q");
                    var res = postgres.autocomplete(ctx.pathParam("graph"), ctx.queryParam("q"));
                    ctx.contentType("application/json");
                    ctx.json(res);
                })
                .exception(ResourceLimits.ResourceLimitException.class, (e, ctx) -> {
                    ctx.status(e.statusCode());
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(Map.of("error", e.getMessage())));
                })
                .exception(HttpResponseException.class, (e, ctx) -> {
                    ctx.status(e.getStatus());
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(Map.of("error", e.getMessage())));
                })
                .exception(Exception.class, (e, ctx) -> {
                    ctx.status(500);
                    ctx.contentType("application/json");
                    ctx.result(gson.toJson(Map.of("error", e.getMessage())));
                    e.printStackTrace();
                })
                .start("0.0.0.0", port);
    }

    private static QueryTemplate getQueryTemplateOrThrow(
        GrebiQueryTemplatesRepo queryTemplates,
        String graph,
        String templateId
    ) {
        return queryTemplates.getQueryTemplates().stream()
            .filter(qt -> qt.id.equals(templateId) && (qt.graphs == null || qt.graphs.contains(graph)))
            .findFirst()
            .orElseThrow(() -> new NotFoundResponse(
                "Query template " + templateId + " not found for graph " + graph
            ));
    }

}
