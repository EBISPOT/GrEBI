package uk.ac.ebi.grebi.repo;

import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.nio.file.*;


public class GrebiQueryTemplatesRepo {

    static final String QUERY_TEMPLATES_PATH = System.getenv("GREBI_QUERY_TEMPLATES_PATH");

    public static String getQueryTemplatesPath() {
        if (QUERY_TEMPLATES_PATH != null)
            return QUERY_TEMPLATES_PATH;
        return "query_templates";
    }

    private volatile List<QueryTemplate> queryTemplates;
    private volatile List<QueryTopic> queryTopics;

    public GrebiQueryTemplatesRepo() {
        reload();
        startWatching();
    }

    public List<QueryTemplate> getQueryTemplates() {
        return queryTemplates;
    }

    public List<QueryTopic> getQueryTopics() {
        return queryTopics;
    }

    private void reload() {
        try {
            List<QueryTemplate> newTemplates = loadQueryTemplates(getQueryTemplatesPath());
            List<QueryTopic> newTopics = loadQueryTopics(getQueryTemplatesPath() + "/_topics.yaml");
            this.queryTemplates = newTemplates;
            this.queryTopics = newTopics;
            System.out.println("Loaded " + newTemplates.size() + " query templates and " + newTopics.size() + " topics");
        } catch (Exception e) {
            System.err.println("Failed to reload query templates: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void startWatching() {
        Path dir = Path.of(getQueryTemplatesPath()).toAbsolutePath();
        Thread watchThread = new Thread(() -> {
            try {
                WatchService watchService = FileSystems.getDefault().newWatchService();
                dir.register(watchService,
                        StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_MODIFY,
                        StandardWatchEventKinds.ENTRY_DELETE);
                System.out.println("Watching query templates directory for changes: " + dir);
                while (true) {
                    WatchKey key = watchService.take();
                    // Drain all pending events
                    for (WatchEvent<?> event : key.pollEvents()) {
                        Path changed = (Path) event.context();
                        System.out.println("Query template file changed: " + changed + " (" + event.kind() + ")");
                    }
                    key.reset();
                    // Brief pause to coalesce rapid successive changes (e.g. editor save)
                    Thread.sleep(500);
                    // Drain any events that arrived during the pause
                    WatchKey extra = watchService.poll();
                    while (extra != null) {
                        extra.pollEvents();
                        extra.reset();
                        extra = watchService.poll();
                    }
                    reload();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("Query template watcher thread interrupted");
            } catch (IOException e) {
                System.err.println("Failed to watch query templates directory: " + e.getMessage());
                e.printStackTrace();
            }
        }, "query-template-watcher");
        watchThread.setDaemon(true);
        watchThread.start();
    }

    private static List<QueryTemplate> loadQueryTemplates(String directoryPath) {
        List<QueryTemplate> templates = new ArrayList<>();
        try {
            DirectoryStream<Path> stream = Files.newDirectoryStream(Path.of(directoryPath), "*.yaml");

            Yaml yaml = new Yaml();

            for (Path file : stream) {
                if (file.getFileName().toString().startsWith("_")) continue;

                System.out.println("Loading query template from " + file.getFileName());

                try (InputStream input = Files.newInputStream(file)) {
                    QueryTemplate qt = yaml.loadAs(input, QueryTemplate.class);
                    qt.id = file.getFileName().toString().replace(".yaml", "");
                    templates.add(qt);
                }
            }
            stream.close();
        } catch (Exception e) {
            throw new RuntimeException("Failed to load query templates", e);
        }
        return Collections.unmodifiableList(templates);
    }

    private static List<QueryTopic> loadQueryTopics(String filePath) {
        try (InputStream input = Files.newInputStream(Paths.get(filePath))) {
            Yaml yaml = new Yaml();
            var data = yaml.loadAs(input, TopicsWrapper.class);
            return Collections.unmodifiableList(data.topics);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load query topics", e);
        }
    }

    public static class TopicsWrapper {
        public List<QueryTopic> topics;
    }

}
