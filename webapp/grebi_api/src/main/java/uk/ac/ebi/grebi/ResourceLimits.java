package uk.ac.ebi.grebi;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public final class ResourceLimits {

    public static final int DEFAULT_PAGE_SIZE = 10;
    public static final int DEFAULT_MAX_PAGE_SIZE = 100;
    public static final long DEFAULT_MAX_PAGE_OFFSET = 10_000;
    public static final int DEFAULT_MAX_VECTOR_RESULTS = 50;
    public static final int DEFAULT_MAX_TEXT_CHARS = 4_096;
    public static final int DEFAULT_MAX_QUERY_STRING_CHARS = 8_192;
    public static final int DEFAULT_MAX_QUERY_PARAM_VALUES = 200;
    public static final long DEFAULT_MAX_REQUEST_BODY_BYTES = 1_048_576;
    public static final int DEFAULT_MAX_RESOLVE_SINGLE_EDGES = 100;
    public static final int DEFAULT_QUERY_TIMEOUT_SECONDS = 60;
    public static final int DEFAULT_RATE_LIMIT_REQUESTS = 300;
    public static final int DEFAULT_RATE_LIMIT_WINDOW_SECONDS = 60;

    private static final ResourceLimits INSTANCE = fromEnvironment();

    private final int defaultPageSize;
    private final int maxPageSize;
    private final long maxPageOffset;
    private final int maxVectorResults;
    private final int maxTextChars;
    private final int maxQueryStringChars;
    private final int maxQueryParamValues;
    private final long maxRequestBodyBytes;
    private final int maxResolveSingleEdges;
    private final int queryTimeoutSeconds;
    private final int rateLimitRequests;
    private final int rateLimitWindowSeconds;
    private final ConcurrentMap<String, RateBucket> rateBuckets = new ConcurrentHashMap<>();
    private final AtomicLong rateLimitChecks = new AtomicLong();

    public ResourceLimits(
        int defaultPageSize,
        int maxPageSize,
        long maxPageOffset,
        int maxVectorResults,
        int maxTextChars,
        int maxQueryStringChars,
        int maxQueryParamValues,
        long maxRequestBodyBytes,
        int maxResolveSingleEdges,
        int queryTimeoutSeconds,
        int rateLimitRequests,
        int rateLimitWindowSeconds
    ) {
        this.defaultPageSize = requirePositive(defaultPageSize, "defaultPageSize");
        this.maxPageSize = requirePositive(maxPageSize, "maxPageSize");
        this.maxPageOffset = requireNonNegative(maxPageOffset, "maxPageOffset");
        this.maxVectorResults = requirePositive(maxVectorResults, "maxVectorResults");
        this.maxTextChars = requirePositive(maxTextChars, "maxTextChars");
        this.maxQueryStringChars = requirePositive(maxQueryStringChars, "maxQueryStringChars");
        this.maxQueryParamValues = requirePositive(maxQueryParamValues, "maxQueryParamValues");
        this.maxRequestBodyBytes = requirePositive(maxRequestBodyBytes, "maxRequestBodyBytes");
        this.maxResolveSingleEdges = requirePositive(maxResolveSingleEdges, "maxResolveSingleEdges");
        this.queryTimeoutSeconds = requirePositive(queryTimeoutSeconds, "queryTimeoutSeconds");
        this.rateLimitRequests = Math.max(0, rateLimitRequests);
        this.rateLimitWindowSeconds = requirePositive(rateLimitWindowSeconds, "rateLimitWindowSeconds");
        if (this.defaultPageSize > this.maxPageSize) {
            throw new IllegalArgumentException("defaultPageSize cannot exceed maxPageSize");
        }
    }

    public static ResourceLimits get() {
        return INSTANCE;
    }

    public static ResourceLimits fromEnvironment() {
        return new ResourceLimits(
            intEnv("GREBI_DEFAULT_PAGE_SIZE", DEFAULT_PAGE_SIZE),
            intEnv("GREBI_MAX_PAGE_SIZE", DEFAULT_MAX_PAGE_SIZE),
            longEnv("GREBI_MAX_PAGE_OFFSET", DEFAULT_MAX_PAGE_OFFSET),
            intEnv("GREBI_MAX_VECTOR_RESULTS", DEFAULT_MAX_VECTOR_RESULTS),
            intEnv("GREBI_MAX_TEXT_CHARS", DEFAULT_MAX_TEXT_CHARS),
            intEnv("GREBI_MAX_QUERY_STRING_CHARS", DEFAULT_MAX_QUERY_STRING_CHARS),
            intEnv("GREBI_MAX_QUERY_PARAM_VALUES", DEFAULT_MAX_QUERY_PARAM_VALUES),
            longEnv("GREBI_MAX_REQUEST_BODY_BYTES", DEFAULT_MAX_REQUEST_BODY_BYTES),
            intEnv("GREBI_MAX_RESOLVE_SINGLE_EDGES", DEFAULT_MAX_RESOLVE_SINGLE_EDGES),
            intEnv("GREBI_QUERY_TIMEOUT_SECONDS", DEFAULT_QUERY_TIMEOUT_SECONDS),
            intEnv("GREBI_RATE_LIMIT_REQUESTS", DEFAULT_RATE_LIMIT_REQUESTS),
            intEnv("GREBI_RATE_LIMIT_WINDOW_SECONDS", DEFAULT_RATE_LIMIT_WINDOW_SECONDS)
        );
    }

    public int maxPageSize() {
        return maxPageSize;
    }

    public long maxPageOffset() {
        return maxPageOffset;
    }

    public int maxVectorResults() {
        return maxVectorResults;
    }

    public int maxTextChars() {
        return maxTextChars;
    }

    public long maxRequestBodyBytes() {
        return maxRequestBodyBytes;
    }

    public int maxResolveSingleEdges() {
        return maxResolveSingleEdges;
    }

    public int queryTimeoutSeconds() {
        return queryTimeoutSeconds;
    }

    public int queryTimeoutMillis() {
        return Math.toIntExact(Duration.ofSeconds(queryTimeoutSeconds).toMillis());
    }

    public PageRequest pageRequest(String pageParam, String sizeParam) {
        return pageRequest(pageNumber(pageParam), pageSize(sizeParam));
    }

    public PageRequest pageRequest(String pageParam, String sizeParam, Sort sort) {
        return pageRequest(pageNumber(pageParam), pageSize(sizeParam), sort);
    }

    public PageRequest pageRequest(int pageNum, int pageSize) {
        validatePage(pageNum, pageSize);
        return PageRequest.of(pageNum, pageSize);
    }

    public PageRequest pageRequest(int pageNum, int pageSize, Sort sort) {
        validatePage(pageNum, pageSize);
        return PageRequest.of(pageNum, pageSize, sort);
    }

    public int vectorLimit(String nParam) {
        int n = parseIntOrDefault(nParam, Math.min(defaultPageSize, maxVectorResults), "n");
        if (n < 1) {
            throw badRequest("n must be at least 1");
        }
        if (n > maxVectorResults) {
            throw badRequest("n may not exceed " + maxVectorResults);
        }
        return n;
    }

    public void validateText(String value, String fieldName) {
        if (value == null) {
            return;
        }
        if (value.length() > maxTextChars) {
            throw badRequest(fieldName + " may not exceed " + maxTextChars + " characters");
        }
    }

    public void validateRequestBody(byte[] body) {
        if (body != null && body.length > maxRequestBodyBytes) {
            throw new ResourceLimitException(413, "Request body may not exceed " + maxRequestBodyBytes + " bytes");
        }
    }

    public void validateResolveSingleEdgesCount(int count) {
        if (count > maxResolveSingleEdges) {
            throw badRequest("resolve_single_edges may not include more than " + maxResolveSingleEdges + " entries");
        }
    }

    public void validateQueryString(String queryString) {
        if (queryString != null && queryString.length() > maxQueryStringChars) {
            throw badRequest("Query string may not exceed " + maxQueryStringChars + " characters");
        }
    }

    public void validateQueryParams(Map<String, List<String>> params) {
        if (params == null) {
            return;
        }
        int totalValues = 0;
        for (var entry : params.entrySet()) {
            validateText(entry.getKey(), "Query parameter name");
            var values = entry.getValue();
            if (values == null) {
                continue;
            }
            totalValues += values.size();
            if (totalValues > maxQueryParamValues) {
                throw badRequest("Query parameters may not contain more than " + maxQueryParamValues + " values");
            }
            for (String value : values) {
                validateText(value, "Query parameter value");
            }
        }
    }

    public void checkRateLimit(String bucketKey) {
        if (rateLimitRequests <= 0) {
            return;
        }
        var key = bucketKey == null || bucketKey.isBlank() ? "unknown" : bucketKey;
        var now = System.currentTimeMillis();
        var windowMillis = rateLimitWindowSeconds * 1_000L;
        var bucket = rateBuckets.computeIfAbsent(key, ignored -> new RateBucket(now + windowMillis));
        synchronized (bucket) {
            if (now >= bucket.resetAtMillis) {
                bucket.count = 0;
                bucket.resetAtMillis = now + windowMillis;
            }
            bucket.count++;
            if (bucket.count > rateLimitRequests) {
                throw new ResourceLimitException(429, "Rate limit exceeded");
            }
        }
        if (rateLimitChecks.incrementAndGet() % 1024 == 0) {
            rateBuckets.entrySet().removeIf(entry -> now >= entry.getValue().resetAtMillis);
        }
    }

    private int pageNumber(String pageParam) {
        int pageNum = parseIntOrDefault(pageParam, 0, "page");
        if (pageNum < 0) {
            throw badRequest("page must be at least 0");
        }
        return pageNum;
    }

    private int pageSize(String sizeParam) {
        int pageSize = parseIntOrDefault(sizeParam, defaultPageSize, "size");
        if (pageSize < 1) {
            throw badRequest("size must be at least 1");
        }
        if (pageSize > maxPageSize) {
            throw badRequest("size may not exceed " + maxPageSize);
        }
        return pageSize;
    }

    private void validatePage(int pageNum, int pageSize) {
        if (pageNum < 0) {
            throw badRequest("pageNum must be at least 0");
        }
        if (pageSize < 1) {
            throw badRequest("pageSize must be at least 1");
        }
        if (pageSize > maxPageSize) {
            throw badRequest("pageSize may not exceed " + maxPageSize);
        }
        long offset = (long) pageNum * pageSize;
        if (offset > maxPageOffset) {
            throw badRequest("Requested page offset may not exceed " + maxPageOffset);
        }
    }

    private int parseIntOrDefault(String value, int defaultValue, String name) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw badRequest(name + " must be an integer");
        }
    }

    private static int intEnv(String key, int defaultValue) {
        var value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    private static long longEnv(String key, long defaultValue) {
        var value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Long.parseLong(value);
    }

    private static int requirePositive(int value, String name) {
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static long requirePositive(long value, String name) {
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static long requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return value;
    }

    private static ResourceLimitException badRequest(String message) {
        return new ResourceLimitException(400, message);
    }

    private static final class RateBucket {
        private int count;
        private long resetAtMillis;

        private RateBucket(long resetAtMillis) {
            this.resetAtMillis = resetAtMillis;
        }
    }

    public static final class ResourceLimitException extends RuntimeException {
        private final int statusCode;

        public ResourceLimitException(int statusCode, String message) {
            super(message);
            this.statusCode = statusCode;
        }

        public int statusCode() {
            return statusCode;
        }
    }
}
