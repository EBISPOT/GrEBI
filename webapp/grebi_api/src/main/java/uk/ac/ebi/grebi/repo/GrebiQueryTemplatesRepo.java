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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.stream.Collectors;


public class GrebiQueryTemplatesRepo {

    private static final Logger logger = LoggerFactory.getLogger(GrebiQueryTemplatesRepo.class);

    static final String QUERY_TEMPLATES_PATH = System.getenv("GREBI_QUERY_TEMPLATES_PATH");

    public static String getQueryTemplatesPath() {
        if (QUERY_TEMPLATES_PATH != null)
            return QUERY_TEMPLATES_PATH;
        return "query_templates";
    }

    // Always non-null: a failed (re)load keeps the last good set, and before the
    // first load there is simply nothing to serve.
    private volatile List<QueryTemplate> queryTemplates = List.of();
    private volatile List<QueryTopic> queryTopics = List.of();
    private final List<Consumer<List<QueryTemplate>>> reloadListeners = new CopyOnWriteArrayList<>();

    private final String path;

    public GrebiQueryTemplatesRepo() {
        this(getQueryTemplatesPath());
    }

    /** Templates from the given directory (tests point this at fixtures). */
    public GrebiQueryTemplatesRepo(String path) {
        this.path = path;
        reload();
        startPolling();
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
            List<QueryTemplate> newTemplates = loadQueryTemplates(path);
            this.queryTemplates = newTemplates;
            logger.info("Loaded {} query templates", newTemplates.size());
            notifyReloadListeners(newTemplates);
        } catch (Exception e) {
            logger.error("Failed to reload query templates; keeping the previous {}", queryTemplates.size(), e);
        }
        try {
            List<QueryTopic> newTopics = loadQueryTopics(path + "/_topics.yaml");
            this.queryTopics = newTopics;
            logger.info("Loaded {} query topics", newTopics.size());
        } catch (Exception e) {
            logger.error("Failed to reload query topics; keeping the previous {}", queryTopics.size(), e);
        }
    }

    private void notifyReloadListeners(List<QueryTemplate> templates) {
        logger.info("Notifying {} query-template reload listeners for {} templates",
                reloadListeners.size(), templates == null ? 0 : templates.size());
        for (var listener : reloadListeners) {
            try {
                listener.accept(templates);
            } catch (Exception e) {
                logger.error("Query template reload listener failed", e);
            }
        }
    }

    // Reload when anything under the templates directory changes. This polls
    // rather than using a WatchService: in production the directory is an NFS
    // mount written to from another host, and inotify never sees those writes.
    // The tree is ~100 small files, so a stat walk every few seconds is cheap.
    static final int POLL_SECONDS = Integer.parseInt(
            System.getenv().getOrDefault("GREBI_QUERY_TEMPLATES_POLL_SECONDS", "5"));

    private void startPolling() {
        if (POLL_SECONDS <= 0) {
            logger.info("Query template polling disabled (GREBI_QUERY_TEMPLATES_POLL_SECONDS={})", POLL_SECONDS);
            return;
        }
        Path rootDir = Path.of(path).toAbsolutePath().normalize();
        Thread pollThread = new Thread(() -> {
            String last = fingerprint(rootDir);
            logger.info("Polling query templates directory tree every {}s for changes: {}", POLL_SECONDS, rootDir);
            while (true) {
                try {
                    Thread.sleep(POLL_SECONDS * 1000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                String now = fingerprint(rootDir);
                if (!now.equals(last)) {
                    logger.info("Query templates changed on disk; reloading");
                    last = now;
                    reload();
                }
            }
        }, "query-template-poller");
        pollThread.setDaemon(true);
        pollThread.start();
    }

    // Path, size and mtime of every regular file in the tree. Any edit, add,
    // remove or rename changes it.
    static String fingerprint(Path rootDir) {
        try (var stream = Files.walk(rootDir)) {
            return stream
                    .filter(Files::isRegularFile)
                    .sorted()
                    .map(p -> {
                        try {
                            var attrs = Files.readAttributes(p, BasicFileAttributes.class);
                            return p + "|" + attrs.size() + "|" + attrs.lastModifiedTime().toMillis();
                        } catch (IOException e) {
                            return p + "|?";
                        }
                    })
                    .collect(Collectors.joining("\n"));
        } catch (IOException e) {
            // Directory missing or unreadable mid-copy: report "no change" so a
            // transient NFS hiccup does not reload an empty tree over a good one.
            logger.warn("Could not scan query templates directory {}: {}", rootDir, e.getMessage());
            return "";
        }
    }

    // A template that does not parse (bad YAML, a key this build does not know,
    // a duplicate id) is skipped with an error so the rest keep working: the
    // templates are edited in place on the server, and one broken file must not
    // take the API down. Only an unreadable directory fails the load.
    static List<QueryTemplate> loadQueryTemplates(String directoryPath) throws IOException {
        List<QueryTemplate> templates = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        Yaml yaml = new Yaml();
        Path rootDir = Path.of(directoryPath).toAbsolutePath().normalize();

        List<Path> templateFiles;
        try (var stream = Files.walk(rootDir)) {
            templateFiles = stream
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".yaml"))
                    .filter(path -> !path.getFileName().toString().startsWith("_"))
                    .sorted(Comparator.comparing(path -> rootDir.relativize(path).toString()))
                    .toList();
        }

        for (Path file : templateFiles) {
            String templateId = file.getFileName().toString().replace(".yaml", "");
            Path relative = rootDir.relativize(file);
            if (!ids.add(templateId)) {
                logger.error("Skipping query template {}: duplicate id '{}'", relative, templateId);
                continue;
            }
            try (InputStream input = Files.newInputStream(file)) {
                QueryTemplate qt = yaml.loadAs(input, QueryTemplate.class);
                qt.id = templateId;
                normaliseOntologyDatasourceNames(qt);
                templates.add(qt);
                logger.debug("Loaded query template {}", relative);
            } catch (Exception e) {
                ids.remove(templateId);
                logger.error("Skipping query template {}: {}", relative, e.getMessage());
            }
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
