package uk.ac.ebi.grebi.db;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Client for the EBI embedding service with local PCA transformation.
 *
 * PCA models are loaded from graph metadata (the "embedding_pca_models" key),
 * not from separate files. When a PCA model name (e.g. "text-embedding-3-small_pca512")
 * is requested, the client calls the embedding service with the base model name
 * and applies the PCA transform locally.
 */
public class EmbeddingServiceClient {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddingServiceClient.class);

    private final String embeddingServiceUrl;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(30))
            .build();
    private final Gson gson = new Gson();

    // PCA model name (e.g. "text-embedding-3-small_pca512") -> PcaModel
    private final Map<String, PcaModel> pcaModels = new LinkedHashMap<>();

    private static final Pattern PCA_PATTERN = Pattern.compile("^(.+)_pca(\\d+)$");

    private static class PcaModel {
        final String baseModelName;
        final int nComponents;
        final double[] mean;
        final double[][] components; // shape = (n_features, n_components)

        PcaModel(String baseModelName, int nComponents, double[] mean, double[][] components) {
            this.baseModelName = baseModelName;
            this.nComponents = nComponents;
            this.mean = mean;
            this.components = components;
        }
    }

    public EmbeddingServiceClient(Map<String, JsonElement> metadata) {
        String url = System.getenv("GREBI_EMBEDDING_SERVICE_URL");
        this.embeddingServiceUrl = (url != null && !url.isEmpty()) ? url : null;

        loadPcaModelsFromMetadata(metadata);
    }

    private void loadPcaModelsFromMetadata(Map<String, JsonElement> metadata) {
        if (metadata == null) return;
        JsonElement pcaElement = metadata.get("embedding_pca_models");
        if (pcaElement == null || !pcaElement.isJsonObject()) return;

        JsonObject pcaObj = pcaElement.getAsJsonObject();
        for (var entry : pcaObj.entrySet()) {
            String pcaModelName = entry.getKey();
            Matcher m = PCA_PATTERN.matcher(pcaModelName);
            if (!m.matches()) {
                logger.warn("PCA model name does not match expected pattern: {}", pcaModelName);
                continue;
            }

            String baseModelName = m.group(1);
            int nComponents = Integer.parseInt(m.group(2));

            JsonObject modelJson = entry.getValue().getAsJsonObject();
            double[] mean = toDoubleArray(modelJson.getAsJsonArray("mean"));
            double[][] components = toDoubleArray2D(modelJson.getAsJsonArray("components"));

            pcaModels.put(pcaModelName, new PcaModel(baseModelName, nComponents, mean, components));
            logger.info("Loaded PCA model: {} (base={}, components={}, features={})",
                    pcaModelName, baseModelName, nComponents, mean.length);
        }
    }

    private static double[] toDoubleArray(JsonArray arr) {
        double[] result = new double[arr.size()];
        for (int i = 0; i < arr.size(); i++) {
            result[i] = arr.get(i).getAsDouble();
        }
        return result;
    }

    private static double[][] toDoubleArray2D(JsonArray arr) {
        double[][] result = new double[arr.size()][];
        for (int i = 0; i < arr.size(); i++) {
            result[i] = toDoubleArray(arr.get(i).getAsJsonArray());
        }
        return result;
    }

    private float[] applyPca(float[] embedding, PcaModel pca) {
        int nFeatures = pca.mean.length;
        int nComponents = pca.nComponents;
        float[] result = new float[nComponents];

        for (int j = 0; j < nComponents; j++) {
            double sum = 0.0;
            for (int i = 0; i < nFeatures; i++) {
                sum += ((double) embedding[i] - pca.mean[i]) * pca.components[i][j];
            }
            result[j] = (float) sum;
        }
        return result;
    }

    /**
     * Get list of PCA model names available (loaded from metadata).
     * These represent models with pre-computed embeddings in the database.
     */
    public List<String> getAvailableModels() {
        return new ArrayList<>(pcaModels.keySet());
    }

    /**
     * Query the embedding service for available models and return the set
     * of PCA model names whose base model the service can actually embed.
     * Returns empty if the service URL is not configured or the service is unreachable.
     */
    public Set<String> getEmbeddableModels() {
        if (embeddingServiceUrl == null) {
            return Set.of();
        }
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(embeddingServiceUrl + "/models"))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            Set<String> serviceModels = new HashSet<>();
            if (response.statusCode() == 200) {
                JsonObject json = gson.fromJson(response.body(), JsonObject.class);
                if (json.has("models") && json.get("models").isJsonArray()) {
                    json.getAsJsonArray("models").forEach(element -> {
                        if (element.isJsonPrimitive()) {
                            serviceModels.add(element.getAsString());
                        }
                    });
                }
            }

            Set<String> embeddable = new LinkedHashSet<>();
            for (var entry : pcaModels.entrySet()) {
                if (serviceModels.contains(entry.getValue().baseModelName)) {
                    embeddable.add(entry.getKey());
                }
            }
            return embeddable;
        } catch (Exception e) {
            logger.warn("Failed to query embedding service models: {}", e.getMessage());
            return Set.of();
        }
    }

    /**
     * Embed a single text. If the model is a PCA model, embeds with the base model
     * and applies the PCA transform locally.
     */
    public float[] embedText(String model, String text) throws IOException {
        return embedTexts(model, List.of(text))[0];
    }

    /**
     * Embed multiple texts with optional PCA transform.
     */
    public float[][] embedTexts(String model, List<String> texts) throws IOException {
        PcaModel pca = pcaModels.get(model);
        String serviceModel = (pca != null) ? pca.baseModelName : model;

        float[][] embeddings = embedTextsFromService(serviceModel, texts);

        if (pca != null) {
            for (int i = 0; i < embeddings.length; i++) {
                embeddings[i] = applyPca(embeddings[i], pca);
            }
        }

        return embeddings;
    }

    private float[][] embedTextsFromService(String model, List<String> texts) throws IOException {
        if (embeddingServiceUrl == null) {
            throw new IOException("Embedding service URL is not configured (set GREBI_EMBEDDING_SERVICE_URL)");
        }

        JsonObject requestBody = new JsonObject();
        requestBody.addProperty("model", model);
        requestBody.add("text", gson.toJsonTree(texts));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(embeddingServiceUrl))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(60))
                .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(requestBody)))
                .build();

        try {
            HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

            if (response.statusCode() == 200) {
                String dimHeader = response.headers().firstValue("x-embedding-dim").orElse(null);
                if (dimHeader == null) {
                    throw new IOException("Missing x-embedding-dim header in response");
                }
                int dimension = Integer.parseInt(dimHeader);

                byte[] binaryData = response.body();
                int expectedBytes = texts.size() * dimension * 4;

                if (binaryData.length != expectedBytes) {
                    throw new IOException("Unexpected response size: got " + binaryData.length +
                            " bytes, expected " + expectedBytes);
                }

                float[][] embeddings = new float[texts.size()][dimension];
                java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(binaryData)
                        .order(java.nio.ByteOrder.LITTLE_ENDIAN);

                for (int i = 0; i < texts.size(); i++) {
                    for (int j = 0; j < dimension; j++) {
                        embeddings[i][j] = buffer.getFloat();
                    }
                }

                return embeddings;
            } else {
                String responseBody = response.body() != null ? new String(response.body()) : "(empty)";
                throw new IOException("Embedding service returned HTTP " + response.statusCode() +
                        ": " + responseBody);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Request interrupted", e);
        }
    }
}
