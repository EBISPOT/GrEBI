package uk.ac.ebi.grebi_cypher_service;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class DatabaseDiscovery {

    static List<Path> findNeo4jHomes(Path searchRoot) {
        List<Path> homes = new ArrayList<>();
        try {
            Files.walkFileTree(searchRoot, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    // A Neo4j home directory has data/databases/neo4j/
                    if (Files.isDirectory(dir.resolve("data/databases/neo4j"))) {
                        homes.add(dir);
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    // Handle the case where dir itself is the "data" directory
                    // (contains databases/neo4j/ directly). The embedded builder
                    // expects home/data/databases/neo4j, so we need the parent.
                    if (Files.isDirectory(dir.resolve("databases/neo4j"))
                            && !"data".equals(dir.getFileName().toString())) {
                        Path parent = dir.getParent();
                        if (parent != null && parent.resolve("data").equals(dir)) {
                            homes.add(parent);
                        } else {
                            homes.add(dir); // best effort — let builder handle it
                        }
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            System.err.println("ERROR walking search path: " + e.getMessage());
        }
        return homes;
    }

    /**
     * Two-phase embedded database discovery:
     *  Phase 1 — open each database with a minimal page cache (8 MB) to discover
     *            the subgraph name and get the node count via the count store.
     *  Phase 2 — close and reopen each database with a proportional share of
     *            the total page cache budget based on node count.
     */
    static void discoverEmbeddedDatabases(Path searchRoot, Map<String, CypherBackend> backends) {
        System.out.println("Searching for Neo4j databases under " + searchRoot);

        List<Path> homes = findNeo4jHomes(searchRoot);
        System.out.println("Found " + homes.size() + " Neo4j home(s): " + homes);
        if (homes.isEmpty()) return;

        // Phase 1: open with minimal cache, count nodes, then close
        record ProbeResult(Path home, String subgraph, long nodeCount) {}
        List<ProbeResult> probes = new ArrayList<>();

        for (Path home : homes) {
            try {
                EmbeddedBackend probe = new EmbeddedBackend(home, 8);
                String sg = probe.getSubgraph();
                if (backends.containsKey(sg)) {
                    System.out.println("WARNING: subgraph '" + sg + "' already loaded, skipping " + home);
                    probe.close();
                    continue;
                }
                long count = probe.countNodes();
                System.out.println("Probed '" + sg + "': " + count + " nodes");
                probe.close();
                probes.add(new ProbeResult(home, sg, count));
            } catch (Exception e) {
                System.err.println("ERROR probing database at " + home + ": " + e.getMessage());
                e.printStackTrace();
            }
        }

        if (probes.isEmpty()) return;

        long totalPageCacheMb = MemoryBudget.getPageCacheBudgetMb(probes.size());
        System.out.println("Total page cache budget: " + totalPageCacheMb + " MB"
                + " (reserving overhead for " + probes.size() + " DBMS instance(s))");

        long totalNodes = probes.stream().mapToLong(ProbeResult::nodeCount).sum();

        // Phase 2: reopen with proportional page cache
        for (ProbeResult p : probes) {
            long cacheMb;
            if (totalNodes > 0) {
                cacheMb = Math.max(8, (long) ((double) p.nodeCount / totalNodes * totalPageCacheMb));
            } else {
                cacheMb = Math.max(8, totalPageCacheMb / probes.size());
            }
            System.out.println("Reopening '" + p.subgraph + "' with " + cacheMb + " MB page cache"
                    + " (" + p.nodeCount + "/" + totalNodes + " nodes)");
            try {
                EmbeddedBackend backend = new EmbeddedBackend(p.home, cacheMb);
                backends.put(p.subgraph, backend);
            } catch (Exception e) {
                System.err.println("ERROR opening embedded database at " + p.home + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}
