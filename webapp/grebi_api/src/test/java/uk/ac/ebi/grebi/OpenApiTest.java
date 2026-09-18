package uk.ac.ebi.grebi;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/** The OpenAPI description: complete, well-formed, and served. */
class OpenApiTest {

    static final Pattern PATH_PARAM = Pattern.compile("\\{([^}]+)}");

    @Test
    void theDescriptionCoversExactlyTheRoutesTheApiRegisters() {
        try (var app = TestApp.start()) {
            var registered = new TreeSet<String>();
            // the endpoints Javalin registered, from its internal router
            for (var handler : app.app.unsafe.internalRouter.allHttpHandlers()) {
                if (handler.endpoint.method.isHttpMethod()) {
                    registered.add(handler.endpoint.method.name() + " " + handler.endpoint.path);
                }
            }
            var described = new TreeSet<>(OpenApi.load().operations());

            var undescribed = new TreeSet<>(registered);
            undescribed.removeAll(described);
            var phantom = new TreeSet<>(described);
            phantom.removeAll(registered);
            assertTrue(undescribed.isEmpty() && phantom.isEmpty(),
                "openapi.yaml must describe every route and nothing else.\n  routes without a description: " + undescribed
                    + "\n  described but not registered: " + phantom);
            assertTrue(registered.size() > 30, "the route list came from Javalin: " + registered.size());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void everyOperationHasASummaryATagAndItsPathParameters() {
        var doc = OpenApi.load().document();
        var components = (Map<String, Object>) doc.get("components");
        var parameterDefs = (Map<String, Object>) components.get("parameters");
        var tags = new LinkedHashSet<String>();
        for (var tag : (List<Map<String, Object>>) doc.get("tags")) {
            tags.add((String) tag.get("name"));
        }

        var paths = (Map<String, Object>) doc.get("paths");
        var problems = new ArrayList<String>();
        for (var path : paths.entrySet()) {
            var item = (Map<String, Object>) path.getValue();
            var pathLevel = parameters((List<Object>) item.getOrDefault("parameters", List.of()), parameterDefs);
            for (var method : OpenApi.HTTP_METHODS) {
                if (!item.containsKey(method)) {
                    continue;
                }
                var where = method.toUpperCase() + " " + path.getKey();
                var op = (Map<String, Object>) item.get(method);
                if (op.get("summary") == null || ((String) op.get("summary")).isBlank()) {
                    problems.add(where + ": no summary");
                }
                if (op.get("operationId") == null) {
                    problems.add(where + ": no operationId");
                }
                var opTags = (List<String>) op.getOrDefault("tags", List.of());
                if (opTags.isEmpty() || !tags.containsAll(opTags)) {
                    problems.add(where + ": tags " + opTags + " are not all declared");
                }
                if (!((Map<String, Object>) op.getOrDefault("responses", Map.of())).containsKey("200")) {
                    problems.add(where + ": no 200 response");
                }
                var declared = new LinkedHashSet<>(pathLevel);
                declared.addAll(parameters((List<Object>) op.getOrDefault("parameters", List.of()), parameterDefs));
                var matcher = PATH_PARAM.matcher(path.getKey());
                while (matcher.find()) {
                    if (!declared.contains("path:" + matcher.group(1))) {
                        problems.add(where + ": path parameter " + matcher.group(1) + " is not declared");
                    }
                }
            }
        }
        assertEquals(List.of(), problems);
    }

    /** "in:name" for each parameter, references resolved; a path parameter must be required. */
    @SuppressWarnings("unchecked")
    static Set<String> parameters(List<Object> list, Map<String, Object> definitions) {
        var names = new LinkedHashSet<String>();
        for (var raw : list) {
            var parameter = (Map<String, Object>) raw;
            if (parameter.containsKey("$ref")) {
                var ref = (String) parameter.get("$ref");
                var key = ref.substring(ref.lastIndexOf('/') + 1);
                parameter = (Map<String, Object>) definitions.get(key);
                assertNotNull(parameter, "unknown parameter reference " + ref);
            }
            if ("path".equals(parameter.get("in"))) {
                assertEquals(Boolean.TRUE, parameter.get("required"), "path parameter " + parameter.get("name") + " must be required");
            }
            names.add(parameter.get("in") + ":" + parameter.get("name"));
        }
        return names;
    }

    @Test
    void theDescriptionIsServedAsJsonAndAsYaml() {
        try (var app = TestApp.start()) {
            var json = app.get("/api/v1/openapi.json");
            assertEquals(200, json.status());
            assertTrue(json.header("Content-Type").startsWith("application/json"), json.header("Content-Type"));
            var doc = json.json().getAsJsonObject();
            assertEquals("3.1.0", doc.get("openapi").getAsString());
            assertEquals("GrEBI API", doc.getAsJsonObject("info").get("title").getAsString());
            assertTrue(doc.getAsJsonObject("paths").has("/api/v1/graphs/{graph}/search"));

            var yaml = app.get("/api/v1/openapi.yaml");
            assertEquals(200, yaml.status());
            assertTrue(yaml.header("Content-Type").startsWith("application/yaml"), yaml.header("Content-Type"));
            assertTrue(yaml.body().startsWith("openapi: 3.1.0"));
        }
    }

    @Test
    void onlyAnOpenApiDocumentIsAccepted() {
        assertThrows(IllegalArgumentException.class, () -> OpenApi.parse("just: yaml"));
        assertEquals(Set.of("GET /x"), OpenApi.parse("openapi: 3.1.0\npaths:\n  /x:\n    get: {}\n").operations());
    }
}
