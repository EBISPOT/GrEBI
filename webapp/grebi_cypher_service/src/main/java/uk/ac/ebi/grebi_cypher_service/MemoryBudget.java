package uk.ac.ebi.grebi_cypher_service;

import java.nio.file.Files;
import java.nio.file.Path;

class MemoryBudget {

    /**
     * Returns the total page cache budget in MB.
     *
     * If GREBI_PAGE_CACHE_MB is set, that value is used directly.
     * Otherwise we auto-detect total available memory (container or system)
     * and subtract:
     *   - JVM max heap (for the service itself: Javalin, Gson, query processing)
     *   - 256 MB per embedded DBMS instance (native overhead: tx state, buffers)
     *   - 512 MB OS/container reserve
     *
     * Neo4j page cache is off-heap (native memory), so the budget must come
     * from memory *outside* the JVM heap.
     */
    static long getPageCacheBudgetMb(int numDbmsInstances) {
        String env = System.getenv("GREBI_PAGE_CACHE_MB");
        if (env != null && !env.isBlank()) {
            return Long.parseLong(env.trim());
        }

        long totalMemoryMb = getTotalMemoryMb();
        long jvmHeapMb = Runtime.getRuntime().maxMemory() / (1024 * 1024);
        long dbmsOverheadMb = 256L * numDbmsInstances;
        long osReserveMb = 512;

        // The JVM commits memory beyond -Xmx: Metaspace, code cache, thread stacks,
        // direct/NIO buffers and GC structures. The old formula ignored this, so
        // pageCache + heap + nonHeap could exceed the cgroup limit and OOM-kill the
        // pod. Reserve a realistic non-heap allowance (configurable).
        long nonHeapReserveMb = Long.parseLong(
            System.getenv().getOrDefault("GREBI_JVM_NONHEAP_RESERVE_MB", "1536"));

        long budget = totalMemoryMb - jvmHeapMb - nonHeapReserveMb - dbmsOverheadMb - osReserveMb;

        System.out.println("Memory layout: total=" + totalMemoryMb + "MB, jvmHeap=" + jvmHeapMb
                + "MB, jvmNonHeapReserve=" + nonHeapReserveMb + "MB"
                + ", dbmsOverhead=" + dbmsOverheadMb + "MB (" + numDbmsInstances + " instances)"
                + ", osReserve=" + osReserveMb + "MB, pageCacheBudget=" + budget + "MB");

        return Math.max(64, budget);
    }

    /**
     * Detect total available memory in MB.
     * Checks cgroup limits (for containers) first, then falls back to OS total.
     */
    static long getTotalMemoryMb() {
        // cgroup v2
        try {
            String v2 = Files.readString(Path.of("/sys/fs/cgroup/memory.max")).trim();
            if (!"max".equals(v2)) {
                return Long.parseLong(v2) / (1024 * 1024);
            }
        } catch (Exception ignored) {}

        // cgroup v1
        try {
            String v1 = Files.readString(Path.of("/sys/fs/cgroup/memory/memory.limit_in_bytes")).trim();
            long limit = Long.parseLong(v1);
            // cgroup v1 returns a very large number when unlimited
            if (limit < 1L << 50) {
                return limit / (1024 * 1024);
            }
        } catch (Exception ignored) {}

        // Fallback: JVM's view of total physical memory
        try {
            long total = ((com.sun.management.OperatingSystemMXBean)
                    java.lang.management.ManagementFactory.getOperatingSystemMXBean())
                    .getTotalMemorySize();
            return total / (1024 * 1024);
        } catch (Exception ignored) {}

        // Last resort: assume 4x the JVM max heap
        return Runtime.getRuntime().maxMemory() / (1024 * 1024) * 4;
    }
}
