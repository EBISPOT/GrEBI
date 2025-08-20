package uk.ac.ebi.grebi;

import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import io.modelcontextprotocol.server.McpAsyncServer;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.transport.HttpServletSseServerTransportProvider;
import io.modelcontextprotocol.server.transport.HttpServletStreamableServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.ServerCapabilities;
import reactor.core.publisher.Mono;
import uk.ac.ebi.grebi.repo.GrebiMetadataRepo;
import uk.ac.ebi.grebi.repo.GrebiNeoRepo;
import uk.ac.ebi.grebi.repo.GrebiQueryTemplatesRepo;
import uk.ac.ebi.grebi.repo.GrebiSolrRepo;

public class GrebiMcpServer {

    HttpServletStreamableServerTransportProvider transportProvider;
    HttpServletSseServerTransportProvider legacyTransportProvider;
    McpAsyncServer mcpServer, legacyMcpServer;

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

        legacyTransportProvider = HttpServletSseServerTransportProvider.builder()
        .messageEndpoint("/api/v1/mcp/message")
        .sseEndpoint("/api/v1/mcp/sse")
        .objectMapper(new ObjectMapper())
        .build();

        var resourceSpec = new McpServerFeatures.AsyncResourceSpecification(
        McpSchema.Resource.builder()
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
        );

        mcpServer = McpServer.async(transportProvider)
            .serverInfo("grebi", "1.0.0")
            .instructions("This is an instance of GrEBI, a server for large, read-only, ontology-mediated, integrated knowledge graphs which can be accessed using the Model Context Protocol (MCP)")
            .capabilities(ServerCapabilities.builder()
                .resources(true, true)
                .tools(true)
                .prompts(true)
                .logging() 
                .completions()
                .build())
            .resources(resourceSpec)
            .build();

        legacyMcpServer = McpServer.async(legacyTransportProvider)
            .serverInfo("grebi", "1.0.0")
            .instructions("This is an instance of GrEBI, a server for large, read-only, ontology-mediated, integrated knowledge graphs which can be accessed using the Model Context Protocol (MCP)")
            .capabilities(ServerCapabilities.builder()
                .resources(true, true)
                .tools(true)
                .prompts(true)
                .logging() 
                .completions()
                .build())
            .resources(resourceSpec)
            .build();
    }

    public HttpServletStreamableServerTransportProvider getTransportProvider() {
        return transportProvider;
    }

    public HttpServletSseServerTransportProvider getLegacyTransportProvider() {
        return legacyTransportProvider;
    }

    
}
