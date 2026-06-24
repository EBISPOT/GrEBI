package uk.ac.ebi.grebi.db;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Normalises IRIs/CURIEs to their canonical CURIE form by driving the native
 * {@code grebi_reprefix} binary over stdio.
 *
 * Modelled on OLS4's TextTaggerService: the prefix map lives in postgres (a
 * single BYTEA blob in the grebi_prefix_map table), is downloaded to a temp file
 * on first use, and a single long-lived subprocess is reused across all calls.
 * stdio access is serialised with a ReentrantLock; the process is restarted if
 * it dies. This replaces the old standalone HTTP prefix service.
 */
public class PrefixService {

    private static final Logger logger = LoggerFactory.getLogger(PrefixService.class);

    private static final String BINARY_NAME =
        System.getenv().getOrDefault("GREBI_REPREFIX_BIN", "grebi_reprefix");

    // Process-wide singleton so a single subprocess is shared across all callers.
    private static volatile PrefixService INSTANCE;

    public static PrefixService get() {
        PrefixService inst = INSTANCE;
        if (inst != null) {
            return inst;
        }
        synchronized (PrefixService.class) {
            if (INSTANCE == null) {
                INSTANCE = new PrefixService();
            }
            return INSTANCE;
        }
    }

    private final GrebiPostgresClient postgresClient = new GrebiPostgresClient();
    private final ReentrantLock lock = new ReentrantLock();

    private Path dbFile;
    private Process process;
    private BufferedWriter processStdin;
    private BufferedReader processStdout;
    private boolean initialised = false;

    private PrefixService() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "grebi-reprefix-shutdown"));
    }

    /**
     * Normalise a batch of IRIs/CURIEs. The result has the same size and order as
     * the input; each entry is the canonical CURIE, or the input unchanged if no
     * prefix matched.
     */
    public List<String> reprefix(List<String> strs) {
        if (strs == null || strs.isEmpty()) {
            return new ArrayList<>();
        }

        lock.lock();
        try {
            ensureRunning();

            for (String s : strs) {
                // The protocol is one line in, one line out; strip any embedded
                // newlines (CURIEs/IRIs never legitimately contain them).
                String sanitised = s.replace('\n', ' ').replace('\r', ' ');
                processStdin.write(sanitised);
                processStdin.newLine();
            }
            processStdin.flush();

            List<String> out = new ArrayList<>(strs.size());
            for (int i = 0; i < strs.size(); i++) {
                String line = processStdout.readLine();
                if (line == null) {
                    throw new IOException("grebi_reprefix produced no output (line " + i
                        + " of " + strs.size() + ")");
                }
                out.add(line);
            }
            return out;
        } catch (IOException e) {
            // The process is likely dead/desynced; drop it so the next call respawns.
            stopProcess();
            throw new RuntimeException("Failed to reprefix: " + strs, e);
        } finally {
            lock.unlock();
        }
    }

    // --- lifecycle (caller holds the lock) ---------------------------------

    private void ensureRunning() throws IOException {
        if (!initialised) {
            dbFile = downloadPrefixMap();
            initialised = true;
        }
        if (process == null || !process.isAlive()) {
            startProcess();
        }
    }

    private void startProcess() throws IOException {
        ProcessBuilder pb = new ProcessBuilder(BINARY_NAME, dbFile.toString());
        pb.redirectErrorStream(false);
        process = pb.start();
        processStdin = new BufferedWriter(new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
        processStdout = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        logger.info("Started {} (pid {})", BINARY_NAME, process.pid());
    }

    private void stopProcess() {
        try { if (processStdin != null) processStdin.close(); } catch (IOException ignored) {}
        try { if (processStdout != null) processStdout.close(); } catch (IOException ignored) {}
        if (process != null) {
            process.destroy();
        }
        process = null;
        processStdin = null;
        processStdout = null;
    }

    private Path downloadPrefixMap() throws IOException {
        try (Connection conn = postgresClient.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "SELECT data FROM grebi_prefix_map WHERE name = 'prefix_map_normalise'")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IOException("No prefix map found in grebi_prefix_map table");
                }
                byte[] data = rs.getBytes(1);
                Path tmp = Files.createTempFile("grebi_prefix_", ".json");
                tmp.toFile().deleteOnExit();
                Files.write(tmp, data);
                logger.info("Loaded prefix map from postgres ({} bytes) into {}", data.length, tmp);
                return tmp;
            }
        } catch (java.sql.SQLException e) {
            throw new IOException("Failed to load prefix map from postgres", e);
        }
    }

    private synchronized void shutdown() {
        lock.lock();
        try {
            stopProcess();
        } finally {
            lock.unlock();
        }
        if (dbFile != null) {
            try { Files.deleteIfExists(dbFile); } catch (IOException ignored) {}
        }
    }
}
