package uk.ac.ebi.grebi.repo;

import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.nio.file.*;


public class GrebiQueryTemplatesRepo {

    static final String QUERY_TEMPLATES_PATH = System.getenv("GREBI_QUERY_TEMPLATES_PATH");

    public static String getQueryTemplatesPath() {
        if (QUERY_TEMPLATES_PATH != null)
            return QUERY_TEMPLATES_PATH;
        return "query_templates";
    }

    public List<QueryTemplate> queryTemplates;
    public List<QueryTopic> queryTopics;

    public GrebiQueryTemplatesRepo() {
        queryTemplates = new ArrayList<>();
        queryTopics = new ArrayList<>();

        loadQueryTemplates("query_templates/");
        loadQueryTopics("query_templates/_topics.yaml");
    }

    private void loadQueryTemplates(String directoryPath) {
        try {
            DirectoryStream<Path> stream = Files.newDirectoryStream(Path.of(directoryPath), "*.yaml");

            Yaml yaml = new Yaml();

            for (Path file : stream) {
                if (file.getFileName().toString().startsWith("_")) continue;

                System.out.println("Loading query template from " + file.getFileName());

                try (InputStream input = Files.newInputStream(file)) {
                    QueryTemplate qt = yaml.loadAs(input, QueryTemplate.class);
                    qt.id = file.getFileName().toString().replace(".yaml", "");
                    queryTemplates.add(qt);
                }
            }
            stream.close();
        } catch (Exception e) {
            throw new RuntimeException("Failed to load query templates", e);
        } 
    }

    private void loadQueryTopics(String filePath) {
        try (InputStream input = Files.newInputStream(Paths.get(filePath))) {
            Yaml yaml = new Yaml();
            var data = yaml.loadAs(input, TopicsWrapper.class);
            queryTopics.addAll(data.topics);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load query topics", e);
        }
    }

    public static class TopicsWrapper {
        public List<QueryTopic> topics;
    }

}
