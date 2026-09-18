package uk.ac.ebi.grebi;

import com.google.gson.Gson;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The OpenAPI description of this API, maintained by hand in openapi.yaml
 * beside the code and served as YAML and as JSON. A test holds it to the
 * routes the API registers, so a route without a description fails the build.
 */
public final class OpenApi {

    static final String RESOURCE = "/openapi.yaml";
    static final List<String> HTTP_METHODS = List.of("get", "post", "put", "delete", "patch", "head", "options");

    private final String yaml;
    private final String json;
    private final Map<String, Object> document;

    private OpenApi(String yaml, Map<String, Object> document) {
        this.yaml = yaml;
        this.document = document;
        this.json = new Gson().toJson(document);
    }

    /** The description shipped with the API. */
    public static OpenApi load() {
        try (InputStream in = OpenApi.class.getResourceAsStream(RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("No " + RESOURCE + " on the classpath");
            }
            return parse(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** A description from its YAML text. */
    @SuppressWarnings("unchecked")
    public static OpenApi parse(String yaml) {
        Object loaded = new Yaml().load(yaml);
        if (!(loaded instanceof Map<?, ?> map) || !map.containsKey("openapi")) {
            throw new IllegalArgumentException("Not an OpenAPI document");
        }
        return new OpenApi(yaml, (Map<String, Object>) map);
    }

    public String yaml() {
        return yaml;
    }

    public String json() {
        return json;
    }

    public Map<String, Object> document() {
        return document;
    }

    /**
     * The operations described, as "METHOD /path" with the path as Javalin
     * writes it, for example "GET /api/v1/graphs/{graph}".
     */
    @SuppressWarnings("unchecked")
    public Set<String> operations() {
        var operations = new LinkedHashSet<String>();
        var paths = (Map<String, Object>) document.getOrDefault("paths", Map.of());
        for (var path : paths.entrySet()) {
            var item = (Map<String, Object>) path.getValue();
            for (var method : HTTP_METHODS) {
                if (item.containsKey(method)) {
                    operations.add(method.toUpperCase(Locale.ROOT) + " " + path.getKey());
                }
            }
        }
        return operations;
    }
}
