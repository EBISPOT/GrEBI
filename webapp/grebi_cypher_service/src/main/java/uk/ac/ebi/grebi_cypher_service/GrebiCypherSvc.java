package uk.ac.ebi.grebi_cypher_service;

import com.google.gson.Gson;
import io.javalin.Javalin;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GrebiCypherSvc {

    static final Gson gson = new Gson();

    static final Map<String, CypherBackend> backends = new ConcurrentHashMap<>();

    private static volatile boolean ready = false;

    public static void main(String[] args) {

        // --- Discover embedded databases from a search path ------------------

        String searchPath = System.getenv("GREBI_NEO4J_DATA_SEARCH_PATH");
        if (searchPath != null && !searchPath.isBlank()) {
            DatabaseDiscovery.discoverEmbeddedDatabases(Path.of(searchPath), backends);
        }

        // --- Connect to bolt hosts (with retry) ------------------------------

        String hosts = System.getenv("GREBI_NEO4J_HOSTS");
        if (hosts != null && !hosts.isBlank()) {
            connectBoltHosts(hosts);
        }

        if (backends.isEmpty() && (hosts == null || hosts.isBlank())) {
            System.err.println("ERROR: No Neo4j databases found. "
                    + "Set GREBI_NEO4J_DATA_SEARCH_PATH and/or GREBI_NEO4J_HOSTS.");
            System.exit(1);
        }

        if (!backends.isEmpty()) {
            ready = true;
            System.out.println("Loaded graphs: " + backends.keySet());
        }

        // --- Start HTTP server -----------------------------------------------

        int port = Integer.parseInt(System.getenv().getOrDefault("GREBI_CYPHER_PORT", "8085"));

        Javalin app = Javalin.create(config -> {
        }).start("0.0.0.0", port);

        app.get("/health", ctx -> {
            ctx.contentType("application/json");
            if (ready) {
                ctx.result("{\"status\":\"ok\"}");
            } else {
                ctx.status(503);
                ctx.result("{\"status\":\"waiting_for_backends\"}");
            }
        });

        app.get("/", ctx -> {
            ctx.contentType("application/json");
            ctx.result(gson.toJson(backends.keySet()));
        });

        app.post("/{graph}", ctx -> {
            String graph = ctx.pathParam("graph");
            CypherBackend backend = backends.get(graph);
            if (backend == null) {
                ctx.status(404);
                ctx.contentType("application/json");
                ctx.result("{\"error\":\"Graph not found: " + graph + "\"}");
                return;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> body = gson.fromJson(ctx.body(), Map.class);
            String query = (String) body.get("query");
            if (query == null || query.isBlank()) {
                ctx.status(400);
                ctx.contentType("application/json");
                ctx.result("{\"error\":\"Missing 'query' field\"}");
                return;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> rawParams =
                    body.containsKey("params") ? (Map<String, Object>) body.get("params") : Map.of();
            Map<String, Object> params = ValueSerializer.convertParams(rawParams);

            ctx.res().setContentType("application/x-ndjson");
            var out = ctx.res().getOutputStream();

            try {
                backend.streamQuery(query, params, out);
                out.flush();
            } catch (Exception e) {
                // If response not yet committed we can set 500; otherwise write an error line
                if (!ctx.res().isCommitted()) {
                    ctx.res().setStatus(500);
                }
                String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
                out.write(gson.toJson(Map.of("_error", msg)).getBytes(StandardCharsets.UTF_8));
                out.write('\n');
                out.flush();
            }
        });

        // --- Shutdown hook ---------------------------------------------------

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (var backend : backends.values()) {
                try {
                    backend.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }));

        // If bolt hosts were specified but none connected yet, retry in background
        if (!ready && hosts != null && !hosts.isBlank()) {
            System.out.println("No backends ready yet — will retry bolt connections in background");
            Thread retryThread = new Thread(() -> retryBoltHosts(hosts), "bolt-retry");
            retryThread.setDaemon(true);
            retryThread.start();
        }
    }

    private static void connectBoltHosts(String hosts) {
        for (String host : hosts.split(";")) {
            host = host.trim();
            if (host.isEmpty()) continue;
            try {
                BoltBackend backend = new BoltBackend(host);
                String sg = backend.getGraph();
                if (backends.containsKey(sg)) {
                    System.out.println("WARNING: graph '" + sg
                            + "' already loaded (embedded), skipping bolt host " + host);
                } else {
                    backends.put(sg, backend);
                }
            } catch (Exception e) {
                System.err.println("Failed to connect to bolt host " + host
                        + ": " + e.getMessage());
            }
        }
    }

    private static void retryBoltHosts(String hosts) {
        int maxAttempts = 30;
        long delaySecs = 5;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                Thread.sleep(delaySecs * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            System.out.println("Retrying bolt connections (attempt " + attempt + "/" + maxAttempts + ")...");
            connectBoltHosts(hosts);
            if (!backends.isEmpty()) {
                ready = true;
                System.out.println("Bolt backends now available: " + backends.keySet());
                return;
            }
        }
        System.err.println("ERROR: Gave up connecting to bolt hosts after " + maxAttempts + " attempts.");
    }
}
