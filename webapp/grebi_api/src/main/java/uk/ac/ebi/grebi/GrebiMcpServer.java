package uk.ac.ebi.grebi;

import java.awt.desktop.QuitResponse;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.util.JsonSchemaCreator;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import io.modelcontextprotocol.server.McpAsyncServer;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.transport.HttpServletSseServerTransportProvider;
import io.modelcontextprotocol.server.transport.HttpServletStreamableServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.Content;
import io.modelcontextprotocol.spec.McpSchema.ServerCapabilities;
import reactor.core.publisher.Mono;
import uk.ac.ebi.grebi.repo.GrebiMetadataRepo;
import uk.ac.ebi.grebi.repo.GrebiNeoRepo;
import uk.ac.ebi.grebi.repo.GrebiQueryTemplatesRepo;
import uk.ac.ebi.grebi.repo.GrebiSolrRepo;

public class GrebiMcpServer {

    HttpServletStreamableServerTransportProvider transportProvider;
    HttpServletSseServerTransportProvider legacyTransportProvider;
    McpAsyncServer mcpServer;

    public static final String INSTRUCTIONS = """
    This is an instance of GrEBI, a server for large, read-only, ontology-mediated, integrated knowledge graphs
    which can be accessed using the Model Context Protocol (MCP). You cannot directly run queries against GrEBI's
    Neo4j and Solr databases. However GrEBI provides query templates which can be accessed via the MCP, and you
    can provide your own parameters to those templates to query the graph. You can also query for specific nodes
    and edges using the MCP to traverse your own way through the graph.
    """;

    public GrebiMcpServer(
        final GrebiNeoRepo neo,
        final GrebiSolrRepo solr,
        final GrebiMetadataRepo metadata,
        final Set<String> subgraphs,
        final GrebiQueryTemplatesRepo queryTemplates
    ) {
        var stats = neo != null ? neo.getStats() : null;

        Gson gson = new Gson();


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
                                gson.toJson(queryTemplates.queryTopics))
                        );
                        return Mono.just(new McpSchema.ReadResourceResult(contents));
                    }
            ),
            new McpServerFeatures.AsyncResourceSpecification( McpSchema.Resource.builder()
                    .uri("grebi://subgraphs")
                    .name("Subgraphs")
                    .mimeType("application/json")
                    .build(),
                    (exchange, request) -> {
                        List<McpSchema.ResourceContents> contents = List.of(
                            new McpSchema.TextResourceContents(
                                request.uri(),
                                "application/json",
                                gson.toJson(subgraphs))
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
                                gson.toJson(queryTemplates.queryTemplates))
                        );
                        return Mono.just(new McpSchema.ReadResourceResult(contents));
                    }
            )              
        ));


        List<McpServerFeatures.AsyncToolSpecification> tools = new ArrayList<>();

        queryTemplates.queryTemplates.forEach(qt -> {

            var paramProps = new LinkedHashMap<String, Object>();

            paramProps.put("subgraph", Map.of(
                "enum", qt.subgraphs.stream().toList()
            ));

            for (var param : qt.params) {
                var paramDef = new LinkedHashMap<String, Object>();
                paramDef.put("type", "string");
                paramDef.put("description", param.param_name); // TODO: add a param_desc
                paramProps.put(param.param_id, paramDef);
            }

            paramProps.put("sortBy", Map.of(
                "enum", qt.result_columns.stream().map(c -> c.column_id).toList()
            ));
            paramProps.put("sortDir", Map.of(
                "enum", List.of("asc", "desc")
            ));
            paramProps.put("pageNum",  Map.of("type", "integer"));
            paramProps.put("pageSize", Map.of("type", "integer"));

            var requiredParams = paramProps.keySet().stream().toList();

            McpSchema.JsonSchema inputSchema = new McpSchema.JsonSchema(
                "object",
                paramProps,
                requiredParams,
                null,
                null,
                null);

            Map<String,Object> rowSchemaProps = new LinkedHashMap<>();
            for(var col : qt.result_columns) {
                // TODO: add description for result cols
                var colDef = new LinkedHashMap<String, Object>();
                if(col.column_type.equalsIgnoreCase("GraphNodeId")) {
                    colDef.put("type", "object"); 
                // } else if(col.column_type.equalsIgnoreCase("float")) {
                //     colDef.put("type", "number");
                // } else if(col.column_type.equalsIgnoreCase("int")) {
                //     colDef.put("type", "number");
                } else {
                    colDef.put("type", "string");
                }
                if(col.optional != null && col.optional) {
                    colDef.put("type", List.of(colDef.get("type"), "null"));
                }
                rowSchemaProps.put(col.column_id, colDef);
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
                    .description(qt.title+": " + qt.description)
                    .inputSchema(inputSchema)
                    .outputSchema(outputSchema)
                    .build(),
                null,
                (exchange, request) -> {

                    var subgraph = (String) request.arguments().get("subgraph");
                    var sortBy = (String) request.arguments().get("sortBy");
                    var sortDir = (String) request.arguments().get("sortDir");
                    var pageNum = (Integer) request.arguments().get("pageNum");
                    var pageSize = (Integer) request.arguments().get("pageSize");

                    var otherParams = new LinkedHashMap<String, Object>(request.arguments());

                    if(!subgraphs.contains(subgraph)) {
                        return Mono.error(new RuntimeException("Unknown subgraph " + subgraph));
                    }

                    if(!List.of("asc", "desc").contains(sortDir)) {
                        return Mono.error(new RuntimeException("Unknown sort direction " + sortDir));
                    }

                    if(!qt.result_columns.stream().map(c -> c.column_id).toList().contains(sortBy)) {
                        return Mono.error(new RuntimeException("Unknown sort column " + sortBy));
                    }

                    var page = PageRequest.of(pageNum, pageSize,
                            Sort.by(sortDir.equals("asc") ? Sort.Direction.ASC : Sort.Direction.DESC, sortBy));

                    Map<String,List<String>> params = new LinkedHashMap<>();
                    for(var p : request.arguments().entrySet()) {
                        if(List.of("subgraph", "sortBy", "sortDir", "pageNum", "pageSize").contains(p.getKey())) {
                            continue;
                        }
                        params.put(p.getKey(), List.of(p.getValue().toString()));
                    }

                    Page<Map<String,Object>> res = neo.runQueryFromTemplatePaginated(subgraph, qt, params, false, page);

                    var result = Map.of(
                        "rows", res.getContent(),
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
    

    
}
