package uk.ac.ebi.grebi.repo;

import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.nio.file.*;


public class GrebiQueryTemplatesRepo {

    private static final Logger logger = LoggerFactory.getLogger(GrebiQueryTemplatesRepo.class);

    static final String QUERY_TEMPLATES_PATH = System.getenv("GREBI_QUERY_TEMPLATES_PATH");

    public static String getQueryTemplatesPath() {
        if (QUERY_TEMPLATES_PATH != null)
            return QUERY_TEMPLATES_PATH;
        return "query_templates";
    }

    private volatile List<QueryTemplate> queryTemplates;
    private volatile List<QueryTopic> queryTopics;
    private final List<Consumer<List<QueryTemplate>>> reloadListeners = new CopyOnWriteArrayList<>();

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

    public void addReloadListener(Consumer<List<QueryTemplate>> listener) {
        if (listener != null) {
            reloadListeners.add(listener);
        }
    }

    private void reload() {
        try {
            List<QueryTemplate> newTemplates = loadQueryTemplates(getQueryTemplatesPath());
            List<QueryTopic> newTopics = loadQueryTopics(getQueryTemplatesPath() + "/_topics.yaml");
            this.queryTemplates = newTemplates;
            this.queryTopics = newTopics;
            System.out.println("Loaded " + newTemplates.size() + " query templates and " + newTopics.size() + " topics");
            notifyReloadListeners(newTemplates);
        } catch (Exception e) {
            System.err.println("Failed to reload query templates: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void notifyReloadListeners(List<QueryTemplate> templates) {
        logger.info("Notifying {} query-template reload listeners for {} templates",
                reloadListeners.size(), templates == null ? 0 : templates.size());
        for (var listener : reloadListeners) {
            try {
                listener.accept(templates);
            } catch (Exception e) {
                System.err.println("Query template reload listener failed: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private void startWatching() {
        Path rootDir = Path.of(getQueryTemplatesPath()).toAbsolutePath();
        Thread watchThread = new Thread(() -> {
            try {
                WatchService watchService = FileSystems.getDefault().newWatchService();
                Set<Path> watchedDirs = new HashSet<>();
                registerDirectoryTree(rootDir, watchService, watchedDirs);
                System.out.println("Watching query templates directory tree for changes: " + rootDir);
                while (true) {
                    WatchKey key = watchService.take();
                    Path watchedDir = (Path) key.watchable();
                    // Drain all pending events
                    for (WatchEvent<?> event : key.pollEvents()) {
                        Path changed = (Path) event.context();
                        Path changedPath = watchedDir.resolve(changed);
                        System.out.println("Query template file changed: " + changedPath + " (" + event.kind() + ")");

                        if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE && Files.isDirectory(changedPath)) {
                            registerDirectoryTree(changedPath, watchService, watchedDirs);
                        }
                    }
                    if (!key.reset()) {
                        watchedDirs.remove(watchedDir);
                    }
                    // Brief pause to coalesce rapid successive changes (e.g. editor save)
                    Thread.sleep(500);
                    // Drain any events that arrived during the pause
                    WatchKey extra = watchService.poll();
                    while (extra != null) {
                        Path extraWatchedDir = (Path) extra.watchable();
                        for (WatchEvent<?> event : extra.pollEvents()) {
                            Path changed = (Path) event.context();
                            Path changedPath = extraWatchedDir.resolve(changed);
                            if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE && Files.isDirectory(changedPath)) {
                                registerDirectoryTree(changedPath, watchService, watchedDirs);
                            }
                        }
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

    private static void registerDirectoryTree(Path rootDir, WatchService watchService, Set<Path> watchedDirs) throws IOException {
        try (var stream = Files.walk(rootDir)) {
            stream.filter(Files::isDirectory)
                    .sorted()
                    .forEach(dir -> registerDirectory(dir, watchService, watchedDirs));
        }
    }

    private static void registerDirectory(Path dir, WatchService watchService, Set<Path> watchedDirs) {
        try {
            Path absoluteDir = dir.toAbsolutePath().normalize();
            if (!watchedDirs.add(absoluteDir)) {
                return;
            }
            absoluteDir.register(
                    watchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_DELETE
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to watch query templates directory " + dir, e);
        }
    }

    private static List<QueryTemplate> loadQueryTemplates(String directoryPath) {
        List<QueryTemplate> templates = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        try {
            Yaml yaml = new Yaml();
            Path rootDir = Path.of(directoryPath).toAbsolutePath().normalize();

            try (var stream = Files.walk(rootDir)) {
                List<Path> templateFiles = stream
                        .filter(Files::isRegularFile)
                        .filter(path -> path.getFileName().toString().endsWith(".yaml"))
                        .filter(path -> !path.getFileName().toString().startsWith("_"))
                        .sorted(Comparator.comparing(path -> rootDir.relativize(path).toString()))
                        .toList();

                for (Path file : templateFiles) {
                    String templateId = file.getFileName().toString().replace(".yaml", "");
                    if (!ids.add(templateId)) {
                        throw new IllegalStateException("Duplicate query template id '" + templateId + "' found at " + rootDir.relativize(file));
                    }

                    System.out.println("Loading query template from " + rootDir.relativize(file));

                    try (InputStream input = Files.newInputStream(file)) {
                        QueryTemplate qt = yaml.loadAs(input, QueryTemplate.class);
                        qt.id = templateId;
                        normaliseOntologyDatasourceNames(qt);
                        templates.add(qt);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load query templates", e);
        }
        return Collections.unmodifiableList(templates);
    }

    // ---------------------------------------------------------------------
    // TRANSITIONAL: ontology datasource naming (remove once every deployed
    // release is on one naming scheme).
    //
    // The OLS-JSON ingest names per-ontology datasources "OLS.<ont>"; the newer
    // owlmake/ubergraph ingest derives them from each term's rdfs:isDefinedBy as
    // "Ontologies.<ont>". Both spellings are in flight — dev and prod currently
    // serve OLS.*, while templates and subgraph configs are written against
    // Ontologies.*. Rather than pin templates to whichever release is live (and
    // break on the mirror image at changeover), we rewrite the datasource
    // predicate at load time to accept either spelling.
    //
    // Matches the one shape templates use:
    //     "<Ontologies|OLS>.<ont>" IN <var>.`grebi:datasources`
    // and rewrites it to:
    //     any(d IN <var>.`grebi:datasources` WHERE d IN ["Ontologies.<ont>", "OLS.<ont>"])
    // ---------------------------------------------------------------------

    private static final java.util.regex.Pattern DATASOURCE_PREDICATE = java.util.regex.Pattern.compile(
            "(['\"])(?:Ontologies|OLS)\\.([A-Za-z0-9_]+)\\1\\s+IN\\s+([A-Za-z_][A-Za-z0-9_]*\\.`grebi:datasources`)");

    // Catches any remaining Ontologies./OLS. literal the rewrite above did not
    // reach, so a template written in an unexpected shape is loud rather than
    // silently filtering everything out.
    private static final java.util.regex.Pattern ANY_ONTOLOGY_DATASOURCE_LITERAL = java.util.regex.Pattern.compile(
            "(['\"])(?:Ontologies|OLS)\\.[A-Za-z0-9_]+\\1");

    static String normaliseOntologyDatasourceNames(String cypher) {
        if (cypher == null || (!cypher.contains("Ontologies.") && !cypher.contains("OLS."))) {
            return cypher;
        }
        var matcher = DATASOURCE_PREDICATE.matcher(cypher);
        StringBuilder out = new StringBuilder();
        while (matcher.find()) {
            String ontology = matcher.group(2);
            String datasourcesExpr = matcher.group(3);
            matcher.appendReplacement(out, java.util.regex.Matcher.quoteReplacement(
                    "any(__grebi_ds IN " + datasourcesExpr + " WHERE __grebi_ds IN "
                            + "[\"Ontologies." + ontology + "\", \"OLS." + ontology + "\"])"));
        }
        matcher.appendTail(out);
        return out.toString();
    }

    // Literals left in the ORIGINAL fragment once the predicates we rewrote are
    // removed. Scanning the rewritten text instead would match the very literals
    // the rewrite emits.
    private static void warnOnUnrecognisedDatasourceLiterals(String templateId, String originalCypher) {
        if (originalCypher == null) {
            return;
        }
        String residue = DATASOURCE_PREDICATE.matcher(originalCypher).replaceAll("");
        var leftover = ANY_ONTOLOGY_DATASOURCE_LITERAL.matcher(residue);
        while (leftover.find()) {
            logger.warn("Query template '{}' contains ontology datasource literal {} in a shape the " +
                            "Ontologies./OLS. normaliser does not recognise; it will only match one naming " +
                            "scheme and may silently return no rows",
                    templateId, leftover.group());
        }
    }

    private static void normaliseOntologyDatasourceNames(QueryTemplate qt) {
        warnOnUnrecognisedDatasourceLiterals(qt.id, qt.cypher_match_fragment);
        warnOnUnrecognisedDatasourceLiterals(qt.id, qt.cypher_return_fragment);
        warnOnUnrecognisedDatasourceLiterals(qt.id, qt.cypher_count_fragment);

        qt.cypher_match_fragment = normaliseOntologyDatasourceNames(qt.cypher_match_fragment);
        qt.cypher_return_fragment = normaliseOntologyDatasourceNames(qt.cypher_return_fragment);
        qt.cypher_count_fragment = normaliseOntologyDatasourceNames(qt.cypher_count_fragment);
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
