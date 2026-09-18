package uk.ac.ebi.grebi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Sort;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import io.modelcontextprotocol.server.McpAsyncServer;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.transport.HttpServletStreamableServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.ServerCapabilities;
import reactor.core.publisher.Mono;
import uk.ac.ebi.grebi.repo.GrebiCypherRepo;
import uk.ac.ebi.grebi.repo.GrebiMetadataRepo;
import uk.ac.ebi.grebi.repo.GrebiPostgresRepo;
import uk.ac.ebi.grebi.repo.GrebiQueryTemplatesRepo;

public class GrebiMcpServer {

    HttpServletStreamableServerTransportProvider transportProvider;
    McpAsyncServer mcpServer;

    public static final String INSTRUCTIONS = """
    This is an instance of GrEBI, a server for large, read-only, ontology-mediated, integrated knowledge graphs
    which can be accessed using the Model Context Protocol (MCP). You cannot directly run queries against GrEBI's
    Neo4j and PostgreSQL databases. However GrEBI provides query templates which can be accessed via the MCP, and you
    can provide your own parameters to those templates to query the graph. You can also search for nodes, inspect
    nodes and edges, and traverse incoming and outgoing edges using dedicated MCP tools.
    """;

    private static Map<String, Object> getColumnSchema(uk.ac.ebi.grebi.repo.QueryTemplate.ResultColumn column) {
        var columnSchema = new LinkedHashMap<String, Object>();

        var columnType = column.column_type == null ? "" : column.column_type.toLowerCase();
        String jsonType;

        if (columnType.equals("graphnodeid")) {
            jsonType = "object";
        } else if (columnType.equals("datasourcelist")) {
            jsonType = "array";
            columnSchema.put("items", Map.of("type", "string"));
        } else if (columnType.equals("float")) {
            jsonType = "number";
        } else if (columnType.equals("int") || columnType.equals("integer")) {
            jsonType = "integer";
        } else if (columnType.equals("boolean")) {
            jsonType = "boolean";
        } else {
            jsonType = "string";
        }

        if (Boolean.TRUE.equals(column.optional)) {
            columnSchema.put("type", List.of(jsonType, "null"));
        } else {
            columnSchema.put("type", jsonType);
        }

        return columnSchema;
    }

    private static McpSchema.JsonSchema buildInputSchema(
        Map<String, Object> properties,
        List<String> requiredProperties
    ) {
        return new McpSchema.JsonSchema(
            "object",
            properties,
            requiredProperties,
            null,
            null,
            null
        );
    }

    private static Map<String, Object> pagedRowsOutputSchema() {
        var properties = new LinkedHashMap<String, Object>();
        properties.put("rows", Map.of(
            "type", "array",
            "items", Map.of("type", "object")
        ));
        properties.put("totalNumRows", Map.of("type", "integer"));
        properties.put("totalNumPages", Map.of("type", "integer"));
        properties.put("pageNum", Map.of("type", "integer"));
        properties.put("pageSize", Map.of("type", "integer"));
        properties.put("facets", Map.of("type", "object"));
        return Map.of(
            "type", "object",
            "properties", properties
        );
    }

    private static Map<String, Object> requireObjectOutputSchema(String key) {
        var properties = new LinkedHashMap<String, Object>();
        properties.put(key, Map.of("type", "object"));
        return Map.of(
            "type", "object",
            "properties", properties,
            "required", List.of(key)
        );
    }

    private static Mono<McpSchema.CallToolResult> toolResult(
        Gson gson,
        Map<String, Object> result,
        Map<String, Object> outputSchema
    ) {
        return Mono.just(
            new McpSchema.CallToolResult(
                List.of(new McpSchema.TextContent(gson.toJson(result))),
                false,
                result,
                outputSchema
            )
        );
    }

    private static void validateGraph(Set<String> graphs, String graph) {
        if (!graphs.contains(graph)) {
            throw new RuntimeException("Unknown graph " + graph);
        }
    }

    private static String requireStringArg(Map<String, Object> args, String key) {
        var value = args.get(key);
        if (value == null || value.toString().isBlank()) {
            throw new RuntimeException("Missing required argument: " + key);
        }
        return value.toString();
    }

    private static String getStringArg(Map<String, Object> args, String key, String defaultValue) {
        var value = args.get(key);
        if (value == null) {
            return defaultValue;
        }
        var str = value.toString();
        return str.isBlank() ? defaultValue : str;
    }

    private static int getIntArg(Map<String, Object> args, String key, int defaultValue) {
        var value = args.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number num) {
            return num.intValue();
        }
        return Integer.parseInt(value.toString());
    }

    private static boolean getBooleanArg(Map<String, Object> args, String key, boolean defaultValue) {
        var value = args.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        return Boolean.parseBoolean(value.toString());
    }

    /** An identifier list argument: an array of strings, or one string with an identifier per line. */
    private static List<String> getIdsArg(Gson gson, Map<String, Object> args, String key) {
        var raw = args.get(key);
        if (raw == null) {
            throw new RuntimeException("Missing required argument: " + key);
        }
        if (raw instanceof List<?> list) {
            return NodeLookup.parseIds(gson.toJson(list));
        }
        if (raw instanceof String text) {
            return NodeLookup.parseIds(text);
        }
        throw new RuntimeException(key + " must be an array of strings");
    }

    private static Map<String, List<String>> getFiltersArg(Map<String, Object> args, String key) {
        var raw = args.get(key);
        if (raw == null) {
            return Collections.emptyMap();
        }
        if (!(raw instanceof Map<?, ?> rawMap)) {
            throw new RuntimeException(key + " must be an object mapping field names to strings or arrays of strings");
        }

        var filters = new LinkedHashMap<String, List<String>>();
        for (var entry : rawMap.entrySet()) {
            var fieldName = entry.getKey().toString();
            var fieldValue = entry.getValue();
            if (fieldValue == null) {
                continue;
            }
            if (fieldValue instanceof List<?> values) {
                filters.put(fieldName, values.stream().map(Object::toString).toList());
            } else {
                filters.put(fieldName, List.of(fieldValue.toString()));
            }
        }
        return filters;
    }

    private static String stripGraphPrefix(String graph, String id) {
        if (id == null) {
            return null;
        }
        if (id.startsWith(graph + ":")) {
            return id.substring(graph.length() + 1);
        }
        return id;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> attachFromTo(Map<String, Object> edge) {
        if (edge == null) {
            return null;
        }

        var retEdge = new LinkedHashMap<String, Object>(edge);
        Object refsObj = edge.get("_refs");
        if (!(refsObj instanceof Map<?, ?> refs)) {
            return retEdge;
        }

        var fromNodeId = edge.get("grebi:fromNodeId");
        var toNodeId = edge.get("grebi:toNodeId");
        if (fromNodeId instanceof String fromId && refs.containsKey(fromId)) {
            retEdge.put("from", refs.get(fromId));
        }
        if (toNodeId instanceof String toId && refs.containsKey(toId)) {
            retEdge.put("to", refs.get(toId));
        }

        return retEdge;
    }

    private static Map<String, Object> pagedResult(Page<Map<String, Object>> page) {
        var result = new LinkedHashMap<String, Object>();
        result.put("rows", page.getContent());
        result.put("totalNumRows", page.getTotalElements());
        result.put("totalNumPages", page.getTotalPages());
        result.put("pageNum", page.getNumber());
        result.put("pageSize", page.getSize());
        if (page instanceof GrebiFacetedResultsPage<?> faceted) {
            result.put("facets", faceted.facetFieldToCounts);
        }
        return result;
    }

    public GrebiMcpServer(
        final GrebiCypherRepo cypher,
        final GrebiPostgresRepo postgres,
        final GrebiMetadataRepo metadata,
        final Set<String> graphs,
        final GrebiQueryTemplatesRepo queryTemplates,
        final NodeLookup lookup
    ) {
        var stats = cypher != null ? cypher.getStats() : null;

        Gson gson = new Gson();
        ResourceLimits limits = ResourceLimits.get();


        transportProvider =
        HttpServletStreamableServerTransportProvider.builder()
        .mcpEndpoint("/api/v1/mcp")
        .disallowDelete(true)
        .objectMapper(new ObjectMapper())
        .build();

        List<McpServerFeatures.AsyncResourceSpecification> resources = new ArrayList<>();
        
        resources.addAll(List.of(
            new McpServerFeatures.AsyncResourceSpecification( McpSchema.Resource.builder()
                    .uri("grebi://stats")
                    .name("Knowledge Graph Statistics")
                    .mimeType("application/json")
                    .build(),
                    (exchange, request) -> {
                        List<McpSchema.ResourceContents> contents = List.of(
                            new McpSchema.TextResourceContents(
                                request.uri(),
                                "application/json",
                                gson.toJson(stats))
                        );
                        return Mono.just(new McpSchema.ReadResourceResult(contents));
                    }
            ),
            new McpServerFeatures.AsyncResourceSpecification( McpSchema.Resource.builder()
                    .uri("grebi://topics")
                    .name("Query Topics")
                    .mimeType("application/json")
                    .build(),
                    (exchange, request) -> {
                        List<McpSchema.ResourceContents> contents = List.of(
                            new McpSchema.TextResourceContents(
                                request.uri(),
                                "application/json",
                                gson.toJson(queryTemplates.getQueryTopics()))
                        );
                        return Mono.just(new McpSchema.ReadResourceResult(contents));
                    }
            ),
            new McpServerFeatures.AsyncResourceSpecification( McpSchema.Resource.builder()
                    .uri("grebi://graphs")
                    .name("Graphs")
                    .mimeType("application/json")
                    .build(),
                    (exchange, request) -> {
                        List<McpSchema.ResourceContents> contents = List.of(
                            new McpSchema.TextResourceContents(
                                request.uri(),
                                "application/json",
                                gson.toJson(graphs))
                        );
                        return Mono.just(new McpSchema.ReadResourceResult(contents));
                    }
            ),
            new McpServerFeatures.AsyncResourceSpecification( McpSchema.Resource.builder()
                    .uri("grebi://query_templates")
                    .name("Query Templates")
                    .mimeType("application/json")
                    .build(),
                    (exchange, request) -> {
                        List<McpSchema.ResourceContents> contents = List.of(
                            new McpSchema.TextResourceContents(
                                request.uri(),
                                "application/json",
                                gson.toJson(queryTemplates.getQueryTemplates()))
                        );
                        return Mono.just(new McpSchema.ReadResourceResult(contents));
                    }
            )              
        ));


        List<McpServerFeatures.AsyncToolSpecification> tools = new ArrayList<>();
        // per template tool, the graphs the template names (null: any graph the instance serves)
        Map<String, List<String>> declaredGraphs = new LinkedHashMap<>();

        queryTemplates.getQueryTemplates().forEach(qt -> {

            // Standalone materialised queries are browsable tables, not callable
            // parameterised tools (no params/result_columns) — skip them here.
            if (qt.isStandaloneMaterialised()) {
                return;
            }
            declaredGraphs.put(qt.id, qt.graphs);

            var paramProps = new LinkedHashMap<String, Object>();
            // no graphs list means the template runs on every graph; no params list
            // means a parameterless query (both keys are optional in the YAML)
            var templateGraphs = qt.graphs == null ? graphs.stream().toList() : qt.graphs.stream().toList();
            var templateParams = qt.params == null ? List.<uk.ac.ebi.grebi.repo.QueryTemplate.Parameter>of() : qt.params;

            paramProps.put("graph", Map.of(
                "enum", templateGraphs
            ));

            for (var param : templateParams) {
                var paramDef = new LinkedHashMap<String, Object>();
                paramDef.put("type", "string");
                paramDef.put("description", param.param_name); // TODO: add a param_desc
                paramProps.put(param.param_id, paramDef);
            }

            paramProps.put("sortBy", Map.of(
                "enum", qt.result_columns.stream().filter(c -> !c.column_type.equalsIgnoreCase("EdgeId")).map(c -> c.column_id).toList()
            ));
            paramProps.put("sortDir", Map.of(
                "enum", List.of("asc", "desc")
            ));
            paramProps.put("pageNum",  Map.of(
                "type", "integer",
                "minimum", 0,
                "description", "Page number (0-based)"
            ));
            paramProps.put("pageSize", Map.of(
                "type", "integer",
                "minimum", 1,
                "maximum", limits.maxPageSize(),
                "description", "Number of results per page"
            ));

            var requiredParams = paramProps.keySet().stream().toList();

            McpSchema.JsonSchema inputSchema = buildInputSchema(paramProps, requiredParams);

            Map<String,Object> rowSchemaProps = new LinkedHashMap<>();
            for(var col : qt.result_columns) {
                if(col.column_type.equalsIgnoreCase("EdgeId")) {
                    continue;
                }
                // TODO: add description for result cols
                rowSchemaProps.put(col.column_id, getColumnSchema(col));
            }


            Map<String,Object> outputSchema = new LinkedHashMap<>();
            outputSchema.put("type", "object");
            outputSchema.put("properties",
                Map.of(
                    "rows",
                    Map.of(
                        "type", "array",
                        "items", Map.of(
                            "type", "object",
                            "properties", rowSchemaProps
                        )
                    ),
                    "totalNumRows", Map.of("type", "integer"),
                    "totalNumPages", Map.of("type", "integer"),
                    "pageNum", Map.of("type", "integer"),
                    "pageSize", Map.of("type", "integer")
                )
            );

            tools.add(new McpServerFeatures.AsyncToolSpecification(
                McpSchema.Tool.builder()
                    .name(qt.id)
                    // a template without a description is described by its title alone
                    .description(qt.description == null || qt.description.isBlank() ? qt.title : qt.title + ": " + qt.description)
                    .inputSchema(inputSchema)
                    .outputSchema(outputSchema)
                    .build(),
                null,
                (exchange, request) -> {
                    limits.checkRateLimit("mcp:tools");

                    var graph = (String) request.arguments().get("graph");
                    var sortBy = (String) request.arguments().get("sortBy");
                    var sortDir = (String) request.arguments().get("sortDir");
                    var pageNum = getIntArg(request.arguments(), "pageNum", 0);
                    var pageSize = getIntArg(request.arguments(), "pageSize", ResourceLimits.DEFAULT_PAGE_SIZE);

                    // the graphs this template supports (its schema's enum), not every graph the server has

                    if(!templateGraphs.contains(graph)) {
                        return Mono.error(new RuntimeException("Unknown graph " + graph));
                    }

                    if(!List.of("asc", "desc").contains(sortDir)) {
                        return Mono.error(new RuntimeException("Unknown sort direction " + sortDir));
                    }

                    if(!qt.result_columns.stream().map(c -> c.column_id).toList().contains(sortBy)) {
                        return Mono.error(new RuntimeException("Unknown sort column " + sortBy));
                    }

                    var page = limits.pageRequest(pageNum, pageSize,
                            Sort.by(sortDir.equals("asc") ? Sort.Direction.ASC : Sort.Direction.DESC, sortBy));

                    Map<String,List<String>> params = new LinkedHashMap<>();
                    for(var p : request.arguments().entrySet()) {
                        if(List.of("graph", "sortBy", "sortDir", "pageNum", "pageSize").contains(p.getKey())) {
                            continue;
                        }
                        var value = p.getValue() == null ? "" : p.getValue().toString();
                        limits.validateText(value, p.getKey());
                        params.put(p.getKey(), List.of(value));
                    }
                    limits.validateQueryParams(params);

                    Page<Map<String,Object>> res = GrebiApi.serveQueryTemplate(cypher, postgres, metadata, graph, qt, params, null, Map.of(), false, page);

                    var edgeIdColumnIds = qt.result_columns.stream()
                        .filter(c -> c.column_type.equalsIgnoreCase("EdgeId"))
                        .map(c -> c.column_id)
                        .collect(java.util.stream.Collectors.toSet());

                    var filteredRows = res.getContent().stream().map(row -> {
                        var filtered = new LinkedHashMap<String, Object>(row);
                        edgeIdColumnIds.forEach(filtered::remove);
                        return filtered;
                    }).toList();

                    var result = Map.of(
                        "rows", filteredRows,
                        "totalNumRows", res.getTotalElements(),
                        "totalNumPages", res.getTotalPages(),
                        "pageNum", res.getNumber(),
                        "pageSize", res.getSize()
                    );

                    return Mono.just(
                        new McpSchema.CallToolResult(
                            List.of(
                                new McpSchema.TextContent(
                                    gson.toJson(result)
                                )
                            ),
                            false,
                            result,
                            outputSchema
                        )
                    );
                }
            ));
        });

        var graphEnum = graphs.stream().toList();
        var pagedRowsOutputSchema = pagedRowsOutputSchema();
        var nodeOutputSchema = requireObjectOutputSchema("node");
        var edgeOutputSchema = requireObjectOutputSchema("edge");
        var countsOutputSchema = requireObjectOutputSchema("counts");

        var searchNodesProps = new LinkedHashMap<String, Object>();
        searchNodesProps.put("graph", Map.of(
            "enum", graphEnum,
            "description", "Graph to search"
        ));
        searchNodesProps.put("q", Map.of(
            "type", "string",
            "description", "Optional text query to search node labels and identifiers"
        ));
        searchNodesProps.put("resolve", Map.of(
            "type", "boolean",
            "description", "Whether to resolve lightweight hits to full node blobs"
        ));
        searchNodesProps.put("exactMatch", Map.of(
            "type", "boolean",
            "description", "Match the whole name (ignoring case) instead of searching by similarity"
        ));
        searchNodesProps.put("lang", Map.of(
            "type", "string",
            "description", "Language of the resolved nodes' labels (e.g. fr): that language first, English as the fallback, other languages dropped. Nodes list the languages they have in grebi:languages."
        ));
        searchNodesProps.put("filters", Map.of(
            "type", "object",
            "description", "Optional node field filters. Values may be a string or an array of strings."
        ));
        searchNodesProps.put("pageNum", Map.of(
            "type", "integer",
            "minimum", 0,
            "description", "Page number (0-based)"
        ));
        searchNodesProps.put("pageSize", Map.of(
            "type", "integer",
            "minimum", 1,
            "maximum", limits.maxPageSize(),
            "description", "Number of results per page"
        ));

        tools.add(new McpServerFeatures.AsyncToolSpecification(
            McpSchema.Tool.builder()
                .name("search_nodes")
                .description("Search for nodes in a graph by text and optional filters.")
                .inputSchema(buildInputSchema(searchNodesProps, List.of("graph")))
                .outputSchema(pagedRowsOutputSchema)
                .build(),
            null,
            (exchange, request) -> {
                limits.checkRateLimit("mcp:tools");
                var graph = requireStringArg(request.arguments(), "graph");
                validateGraph(graphs, graph);

                var q = getStringArg(request.arguments(), "q", null);
                var resolve = getBooleanArg(request.arguments(), "resolve", true);
                var exactMatch = getBooleanArg(request.arguments(), "exactMatch", false);
                var lang = getStringArg(request.arguments(), "lang", null);
                var pageNum = getIntArg(request.arguments(), "pageNum", 0);
                var pageSize = getIntArg(request.arguments(), "pageSize", ResourceLimits.DEFAULT_PAGE_SIZE);
                var filters = getFiltersArg(request.arguments(), "filters");
                limits.validateText(q, "q");
                limits.validateQueryParams(filters);

                var page = limits.pageRequest(pageNum, pageSize);
                var nodes = postgres.searchNodesPaginated(graph, q, filters, exactMatch, resolve, page);
                var result = pagedResult(resolve ? nodes.map(node -> Languages.localise(node, lang)) : nodes);
                return toolResult(gson, result, pagedRowsOutputSchema);
            }
        ));

        var lookupProps = new LinkedHashMap<String, Object>();
        lookupProps.put("graph", Map.of(
            "enum", graphEnum,
            "description", "Graph to look the identifiers up in"
        ));
        lookupProps.put("ids", Map.of(
            "type", "array",
            "items", Map.of("type", "string"),
            "maxItems", limits.maxLookupIds(),
            "description", "Identifiers to look up: CURIEs such as mondo:0005083 or HGNC:1100, IRIs, or database accessions. A single string with one identifier per line is accepted too."
        ));
        lookupProps.put("matchNames", Map.of(
            "type", "boolean",
            "description", "Also match node names, ignoring case (slower; off by default)"
        ));
        lookupProps.put("resolve", Map.of(
            "type", "boolean",
            "description", "Return full node blobs instead of lightweight hits (off by default)"
        ));
        lookupProps.put("lang", Map.of(
            "type", "string",
            "description", "Language of the resolved nodes' labels (e.g. fr): that language first, English as the fallback, other languages dropped."
        ));
        var lookupOutputProps = new LinkedHashMap<String, Object>();
        lookupOutputProps.put("results", Map.of(
            "type", "array",
            "items", Map.of("type", "object"),
            "description", "One entry per identifier, in the order given: {id, nodes}"
        ));
        lookupOutputProps.put("notFound", Map.of(
            "type", "array",
            "items", Map.of("type", "string"),
            "description", "The identifiers that name no node"
        ));
        lookupOutputProps.put("truncated", Map.of(
            "type", "boolean",
            "description", "True when the lookup hit its row limit, so some matches may be missing"
        ));
        var lookupOutputSchema = Map.<String, Object>of(
            "type", "object",
            "properties", lookupOutputProps,
            "required", List.of("results", "notFound")
        );

        tools.add(new McpServerFeatures.AsyncToolSpecification(
            McpSchema.Tool.builder()
                .name("lookup_nodes")
                .description("Look up many identifiers at once: the node each CURIE, IRI or accession names, and which identifiers name nothing. Prefix case does not matter and IRIs are compacted to the graph's CURIEs.")
                .inputSchema(buildInputSchema(lookupProps, List.of("graph", "ids")))
                .outputSchema(lookupOutputSchema)
                .build(),
            null,
            (exchange, request) -> {
                limits.checkRateLimit("mcp:tools");
                var graph = requireStringArg(request.arguments(), "graph");
                validateGraph(graphs, graph);
                var ids = getIdsArg(gson, request.arguments(), "ids");
                var matchNames = getBooleanArg(request.arguments(), "matchNames", false);
                var resolve = getBooleanArg(request.arguments(), "resolve", false);
                var lang = getStringArg(request.arguments(), "lang", null);
                var result = lookup.lookup(graph, ids, matchNames, resolve, lang);
                return toolResult(gson, result, lookupOutputSchema);
            }
        ));

        var getNodeProps = new LinkedHashMap<String, Object>();
        getNodeProps.put("graph", Map.of(
            "enum", graphEnum,
            "description", "Graph containing the node"
        ));
        getNodeProps.put("nodeId", Map.of(
            "type", "string",
            "description", "Node identifier without Base64 encoding"
        ));
        getNodeProps.put("lang", Map.of(
            "type", "string",
            "description", "Language of the node's labels (e.g. fr): that language first, English as the fallback, other languages dropped. The node lists the languages it has in grebi:languages."
        ));

        tools.add(new McpServerFeatures.AsyncToolSpecification(
            McpSchema.Tool.builder()
                .name("get_node")
                .description("Resolve a specific node by its node ID.")
                .inputSchema(buildInputSchema(getNodeProps, List.of("graph", "nodeId")))
                .outputSchema(nodeOutputSchema)
                .build(),
            null,
            (exchange, request) -> {
                limits.checkRateLimit("mcp:tools");
                var graph = requireStringArg(request.arguments(), "graph");
                var nodeId = requireStringArg(request.arguments(), "nodeId");
                validateGraph(graphs, graph);
                limits.validateText(nodeId, "nodeId");

                var resolved = postgres.getPgClient().resolveToList(graph, List.of(nodeId));
                var node = resolved.isEmpty() ? null : resolved.get(0);
                if (node == null) {
                    return Mono.error(new RuntimeException("Node not found"));
                }

                var lang = getStringArg(request.arguments(), "lang", null);
                return toolResult(gson, Map.of("node", Languages.localise(node, lang)), nodeOutputSchema);
            }
        ));

        var edgeCountsProps = new LinkedHashMap<String, Object>();
        edgeCountsProps.put("graph", Map.of(
            "enum", graphEnum,
            "description", "Graph containing the node"
        ));
        edgeCountsProps.put("nodeId", Map.of(
            "type", "string",
            "description", "Node identifier without Base64 encoding"
        ));
        edgeCountsProps.put("direction", Map.of(
            "enum", List.of("both", "incoming", "outgoing"),
            "description", "Which edge-count direction to return"
        ));

        tools.add(new McpServerFeatures.AsyncToolSpecification(
            McpSchema.Tool.builder()
                .name("get_node_edge_counts")
                .description("Get incoming and/or outgoing edge counts for a node, grouped by edge type and datasource.")
                .inputSchema(buildInputSchema(edgeCountsProps, List.of("graph", "nodeId")))
                .outputSchema(countsOutputSchema)
                .build(),
            null,
            (exchange, request) -> {
                limits.checkRateLimit("mcp:tools");
                var graph = requireStringArg(request.arguments(), "graph");
                var nodeId = requireStringArg(request.arguments(), "nodeId");
                var direction = getStringArg(request.arguments(), "direction", "both");
                validateGraph(graphs, graph);
                limits.validateText(nodeId, "nodeId");

                Object counts;
                switch (direction) {
                    case "incoming" -> counts = postgres.getIncomingEdgeCounts(graph, nodeId);
                    case "outgoing" -> counts = postgres.getOutgoingEdgeCounts(graph, nodeId);
                    case "both" -> counts = postgres.getBothEdgeCounts(graph, nodeId);
                    default -> {
                        return Mono.error(new RuntimeException("Unknown direction " + direction));
                    }
                }

                return toolResult(gson, Map.of("counts", counts), countsOutputSchema);
            }
        ));

        var listNodeEdgesProps = new LinkedHashMap<String, Object>();
        listNodeEdgesProps.put("graph", Map.of(
            "enum", graphEnum,
            "description", "Graph containing the node"
        ));
        listNodeEdgesProps.put("nodeId", Map.of(
            "type", "string",
            "description", "Node identifier without Base64 encoding"
        ));
        listNodeEdgesProps.put("direction", Map.of(
            "enum", List.of("incoming", "outgoing"),
            "description", "Which side of the node to traverse"
        ));
        listNodeEdgesProps.put("refsOnly", Map.of(
            "type", "boolean",
            "description", "Return lightweight edge refs instead of full edge blobs"
        ));
        listNodeEdgesProps.put("filters", Map.of(
            "type", "object",
            "description", "Optional edge field filters. Values may be a string or an array of strings."
        ));
        listNodeEdgesProps.put("sortBy", Map.of(
            "type", "string",
            "description", "Field to sort by"
        ));
        listNodeEdgesProps.put("sortDir", Map.of(
            "enum", List.of("asc", "desc"),
            "description", "Sort direction"
        ));
        listNodeEdgesProps.put("pageNum", Map.of(
            "type", "integer",
            "minimum", 0,
            "description", "Page number (0-based)"
        ));
        listNodeEdgesProps.put("pageSize", Map.of(
            "type", "integer",
            "minimum", 1,
            "maximum", limits.maxPageSize(),
            "description", "Number of results per page"
        ));

        tools.add(new McpServerFeatures.AsyncToolSpecification(
            McpSchema.Tool.builder()
                .name("list_node_edges")
                .description("List incoming or outgoing edges for a node, with optional filters and lightweight edge-ref mode.")
                .inputSchema(buildInputSchema(listNodeEdgesProps, List.of("graph", "nodeId", "direction")))
                .outputSchema(pagedRowsOutputSchema)
                .build(),
            null,
            (exchange, request) -> {
                limits.checkRateLimit("mcp:tools");
                var graph = requireStringArg(request.arguments(), "graph");
                var nodeId = requireStringArg(request.arguments(), "nodeId");
                var direction = requireStringArg(request.arguments(), "direction");
                var refsOnly = getBooleanArg(request.arguments(), "refsOnly", false);
                var sortBy = getStringArg(request.arguments(), "sortBy", "grebi:type");
                var sortDir = getStringArg(request.arguments(), "sortDir", "asc");
                var pageNum = getIntArg(request.arguments(), "pageNum", 0);
                var pageSize = getIntArg(request.arguments(), "pageSize", ResourceLimits.DEFAULT_PAGE_SIZE);
                var filters = getFiltersArg(request.arguments(), "filters");
                validateGraph(graphs, graph);
                limits.validateText(nodeId, "nodeId");
                limits.validateText(sortBy, "sortBy");
                limits.validateQueryParams(filters);

                if (!List.of("incoming", "outgoing").contains(direction)) {
                    return Mono.error(new RuntimeException("Unknown direction " + direction));
                }
                if (!List.of("asc", "desc").contains(sortDir)) {
                    return Mono.error(new RuntimeException("Unknown sort direction " + sortDir));
                }

                var filterField = direction.equals("incoming") ? "grebi:toNodeId" : "grebi:fromNodeId";
                var page = limits.pageRequest(
                    pageNum,
                    pageSize,
                    Sort.by(sortDir.equals("asc") ? Sort.Direction.ASC : Sort.Direction.DESC, sortBy)
                );

                Page<Map<String, Object>> edgePage = refsOnly
                    ? postgres.searchEdgeRefsPaginated(graph, filterField, nodeId, filters, sortBy, sortDir, page)
                    : postgres.searchEdgesPaginated(graph, filterField, nodeId, filters, sortBy, sortDir, page);

                return toolResult(gson, pagedResult(edgePage), pagedRowsOutputSchema);
            }
        ));

        var getEdgeProps = new LinkedHashMap<String, Object>();
        getEdgeProps.put("graph", Map.of(
            "enum", graphEnum,
            "description", "Graph containing the edge"
        ));
        getEdgeProps.put("edgeId", Map.of(
            "type", "string",
            "description", "Edge identifier, with or without the graph prefix"
        ));

        tools.add(new McpServerFeatures.AsyncToolSpecification(
            McpSchema.Tool.builder()
                .name("get_edge")
                .description("Resolve a specific edge by its edge ID.")
                .inputSchema(buildInputSchema(getEdgeProps, List.of("graph", "edgeId")))
                .outputSchema(edgeOutputSchema)
                .build(),
            null,
            (exchange, request) -> {
                limits.checkRateLimit("mcp:tools");
                var graph = requireStringArg(request.arguments(), "graph");
                var edgeId = requireStringArg(request.arguments(), "edgeId");
                validateGraph(graphs, graph);
                limits.validateText(edgeId, "edgeId");

                var cleanEdgeId = stripGraphPrefix(graph, edgeId);
                var resolved = postgres.getPgClient().resolveToMap(graph, List.of(cleanEdgeId));
                var edge = resolved.get(cleanEdgeId);
                if (edge == null) {
                    return Mono.error(new RuntimeException("Edge not found"));
                }

                return toolResult(gson, Map.of("edge", attachFromTo(edge)), edgeOutputSchema);
            }
        ));


        catalogue = buildCatalogue(tools, resources, graphs, declaredGraphs);

        mcpServer = McpServer.async(transportProvider)
            .serverInfo("grebi", "1.0.0")
            .instructions(INSTRUCTIONS)
            .capabilities(ServerCapabilities.builder()
                .resources(true, true)
                .tools(true)
                .prompts(true)
                .logging() 
                .completions()
                .build())
            .tools(tools)
            .resources(resources)
            .build();
    }

    public HttpServletStreamableServerTransportProvider getTransportProvider() {
        return transportProvider;
    }

    private final Map<String, Object> catalogue;

    /**
     * What this server publishes, as plain JSON for the docs: its instructions,
     * the graphs it serves, its resources, and every tool with the schemas
     * handed to tools/list. A tool made from a query template carries the
     * template's id and the graphs the template names; without "graphs" it runs
     * on any graph the instance serves.
     */
    public Map<String, Object> catalogue() {
        return catalogue;
    }

    private static Map<String, Object> buildCatalogue(
        List<McpServerFeatures.AsyncToolSpecification> tools,
        List<McpServerFeatures.AsyncResourceSpecification> resources,
        Set<String> graphs,
        Map<String, List<String>> declaredGraphs
    ) {
        var out = new LinkedHashMap<String, Object>();
        var server = new LinkedHashMap<String, Object>();
        server.put("name", "grebi");
        server.put("version", "1.0.0");
        server.put("endpoint", "/api/v1/mcp");
        server.put("transport", "streamable-http");
        out.put("server", server);
        out.put("instructions", INSTRUCTIONS.strip());
        out.put("graphs", List.copyOf(graphs));

        var resourceList = new ArrayList<Map<String, Object>>();
        for (var spec : resources) {
            var resource = spec.resource();
            var entry = new LinkedHashMap<String, Object>();
            entry.put("uri", resource.uri());
            entry.put("name", resource.name());
            if (resource.description() != null) {
                entry.put("description", resource.description());
            }
            entry.put("mimeType", resource.mimeType());
            resourceList.add(entry);
        }
        out.put("resources", resourceList);

        var toolList = new ArrayList<Map<String, Object>>();
        for (var spec : tools) {
            var tool = spec.tool();
            var entry = new LinkedHashMap<String, Object>();
            entry.put("name", tool.name());
            entry.put("description", tool.description());
            var input = new LinkedHashMap<String, Object>();
            var schema = tool.inputSchema();
            if (schema != null) {
                input.put("type", schema.type());
                input.put("properties", schema.properties() == null ? Map.of() : schema.properties());
                input.put("required", schema.required() == null ? List.of() : schema.required());
            }
            entry.put("inputSchema", input);
            if (tool.outputSchema() != null) {
                entry.put("outputSchema", tool.outputSchema());
            }
            if (declaredGraphs.containsKey(tool.name())) {
                entry.put("template", tool.name());
                var declared = declaredGraphs.get(tool.name());
                if (declared != null) {
                    entry.put("graphs", List.copyOf(declared));
                }
            }
            toolList.add(entry);
        }
        out.put("tools", toolList);
        return out;
    }
    

    
}
