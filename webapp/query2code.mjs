/**
 * Generate code snippets for a GrEBI query template endpoint.
 *
 * Shared between the React UI query pages, docs components, and PDF generator.
 * Plain JS (ESM) so it works in both esbuild and standalone Node.
 *
 * @param {string} baseUrl - Instance base URL (e.g. "http://localhost:8090")
 * @param {string} graph - Graph name
 * @param {string} queryId - Query template ID
 * @param {string[]} paramIds - Parameter names in order
 * @param {Record<string,string>} paramValues - Current parameter values
 */
export default function query2code(baseUrl, graph, queryId, paramIds, paramValues) {
  const base = baseUrl.replace(/\/+$/, "");
  const endpoint = `${base}/api/v1/graphs/${graph}/query/${queryId}`;

  // ── cURL ─────────────────────────────────────────────────────────
  const curlLines = [];
  if (paramIds.length === 0) {
    curlLines.push(`curl '${endpoint}.csv'`);
  } else {
    curlLines.push(`curl -G '${endpoint}.csv'`);
    paramIds.forEach((p) => {
      curlLines[curlLines.length - 1] += " \\";
      curlLines.push(`  --data-urlencode '${p}=${paramValues[p] ?? ""}'`);
    });
  }

  // ── Python ───────────────────────────────────────────────────────
  const pyParams = paramIds.length > 0
    ? `?${paramIds.map((p) => `${p}={${p}}`).join("&")}`
    : "";
  const pythonLines = [
    "import requests",
    "import pandas as pd",
    "from io import StringIO",
    "",
    `def ${queryId}(${paramIds.join(", ")}):`,
    `    url = f"${endpoint}.csv${pyParams}"`,
    "    return pd.read_csv(StringIO(requests.get(url).text))",
    "",
    `print(${queryId}(${paramIds.map((p) => JSON.stringify(paramValues[p] ?? "")).join(", ")}))`,
  ];

  // ── R ────────────────────────────────────────────────────────────
  const rLines = [
    "library(httr)",
    "library(readr)",
    "",
    `${queryId} <- function(${paramIds.join(", ")}) {`,
    `  url <- "${endpoint}.csv"`,
  ];
  if (paramIds.length > 0) {
    rLines.push(`  response <- GET(url, query = list(`);
    paramIds.forEach((p, i) => {
      rLines.push(`    ${p} = ${p}${i < paramIds.length - 1 ? "," : ""}`);
    });
    rLines.push("  ))");
  } else {
    rLines.push("  response <- GET(url)");
  }
  rLines.push(
    `  read_csv(content(response, as = "text", encoding = "UTF-8"))`,
    "}",
    "",
    `print(${queryId}(${paramIds.map((p) => JSON.stringify(paramValues[p] ?? "")).join(", ")}))`
  );

  return {
    cURL: { source: curlLines.join("\n"), lang: "bash" },
    Python: { source: pythonLines.join("\n"), lang: "python" },
    R: { source: rLines.join("\n"), lang: "r" },
  };
}
