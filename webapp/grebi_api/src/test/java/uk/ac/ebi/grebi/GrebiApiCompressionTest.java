package uk.ac.ebi.grebi;

import io.javalin.Javalin;
import io.javalin.compression.CompressionStrategy;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class GrebiApiCompressionTest {

    private static final String LARGE_JSON = "{\"data\":\"" + "x".repeat(4096) + "\"}";

    @Test
    void gzipCompressionStillWorksAndIdentityRequestsStaySuccessful() throws Exception {
        var port = findFreePort();
        var app = Javalin.create(config -> {
                config.http.compressionStrategy = CompressionStrategy.GZIP;
                config.routes
                    .get("/payload", ctx -> {
                        ctx.contentType("application/json");
                        ctx.result(LARGE_JSON);
                    })
                    .get("/events", ctx -> {
                        ctx.contentType("text/event-stream");
                        ctx.result("data: hello\n\n");
                    });
            })
            .start("127.0.0.1", port);

        try (CloseableHttpClient client = HttpClientBuilder.create().disableContentCompression().build()) {
            assertGzipResponse(client, port);
            assertIdentityResponse(client, port);
            assertEventStreamResponse(client, port);
        } finally {
            app.stop();
        }
    }

    private static void assertGzipResponse(CloseableHttpClient client, int port) throws IOException {
        var request = new HttpGet("http://127.0.0.1:" + port + "/payload");
        request.setHeader(HttpHeaders.ACCEPT_ENCODING, "gzip");

        try (var response = client.execute(request)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            var encoding = response.getFirstHeader(HttpHeaders.CONTENT_ENCODING);
            assertNotNull(encoding);
            assertEquals("gzip", encoding.getValue());

            var compressedBody = EntityUtils.toByteArray(response.getEntity());
            assertEquals(LARGE_JSON, gunzip(compressedBody));
        }
    }

    private static void assertIdentityResponse(CloseableHttpClient client, int port) throws IOException {
        var request = new HttpGet("http://127.0.0.1:" + port + "/payload");
        request.setHeader(HttpHeaders.ACCEPT_ENCODING, "identity");

        try (var response = client.execute(request)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertNull(response.getFirstHeader(HttpHeaders.CONTENT_ENCODING));
            assertEquals(LARGE_JSON, EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
        }
    }

    private static void assertEventStreamResponse(CloseableHttpClient client, int port) throws IOException {
        var request = new HttpGet("http://127.0.0.1:" + port + "/events");
        request.setHeader(HttpHeaders.ACCEPT_ENCODING, "gzip");

        try (var response = client.execute(request)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertNull(response.getFirstHeader(HttpHeaders.CONTENT_ENCODING));
            assertEquals("data: hello\n\n", EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
        }
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static String gunzip(byte[] bytes) throws IOException {
        try (var input = new GZIPInputStream(new ByteArrayInputStream(bytes))) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
