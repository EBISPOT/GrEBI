package uk.ac.ebi.grebi;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.transport.HttpServletStreamableServerTransportProvider;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the MCP streamable transport servlet (the same class GrebiApi mounts)
 * actually works when mounted on Javalin 7 / Jetty 12 ee10 — i.e. the jetty-11 ->
 * jetty-12 servlet-API migration didn't break the /api/v1/mcp endpoint.
 */
class GrebiMcpEndpointTest {

    @Test
    void mcpInitializeWorksUnderJavalin7() throws Exception {
        var transportProvider = HttpServletStreamableServerTransportProvider.builder()
            .mcpEndpoint("/api/v1/mcp")
            .objectMapper(new ObjectMapper())
            .build();

        // Minimal MCP server attached to the transport so it can answer `initialize`.
        var mcp = McpServer.async(transportProvider)
            .serverInfo("grebi-test", "1.0.0")
            .build();

        int port = freePort();
        var app = Javalin.create(config -> {
                config.jetty.modifyServletContextHandler(ctx -> {
                    var holder = new ServletHolder(transportProvider);
                    holder.setAsyncSupported(true);
                    ctx.addServlet(holder, "/api/v1/mcp");
                });
            })
            .start("127.0.0.1", port);

        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            var post = new HttpPost("http://127.0.0.1:" + port + "/api/v1/mcp");
            post.setHeader("Content-Type", "application/json");
            post.setHeader("Accept", "application/json, text/event-stream");
            post.setEntity(new StringEntity(
                "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{"
                + "\"protocolVersion\":\"2024-11-05\",\"capabilities\":{},"
                + "\"clientInfo\":{\"name\":\"test\",\"version\":\"1.0\"}}}"));

            try (var resp = client.execute(post)) {
                int sc = resp.getStatusLine().getStatusCode();
                String body = resp.getEntity() != null
                    ? EntityUtils.toString(resp.getEntity()) : "";
                System.out.println("MCP initialize -> HTTP " + sc + " : " + body);
                assertEquals(200, sc,
                    "MCP endpoint should answer 200 under Javalin 7 / Jetty 12; body=" + body);
                assertTrue(
                    body.contains("\"result\"") || body.contains("serverInfo") || body.contains("grebi-test"),
                    "MCP initialize should return a JSON-RPC result; got: " + body);
            }
        } finally {
            app.stop();
            mcp.close();
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) { return s.getLocalPort(); }
    }
}
