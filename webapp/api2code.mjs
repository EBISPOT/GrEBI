/**
 * Generate code snippets in multiple languages for a given API endpoint.
 * 
 * Shared between the React UI components and the PDF generator.
 * Plain JS (ESM) so it works in both esbuild and standalone Node.
 *
 * @param {string} method - HTTP method
 * @param {string} baseUrl - Instance base URL (e.g. "http://localhost:8090")
 * @param {string} path - API path (e.g. "/api/v1/graphs")
 * @param {Record<string,string>} params - Query parameters
 */
export default function api2code(method, baseUrl, path, params) {
  const base = baseUrl.replace(/\/+$/, "");
  const qs = new URLSearchParams(params).toString();
  const fullUrl = qs ? `${base}${path}?${qs}` : `${base}${path}`;

  const curl = `curl "${fullUrl}"`;

  const pythonLines = [
    `import requests`,
    ``,
    `response = requests.${method.toLowerCase()}(`,
    `    "${base}${path}"${Object.keys(params).length > 0 ? "," : ""}`,
  ];
  if (Object.keys(params).length > 0) {
    pythonLines.push(`    params={`);
    for (const [k, v] of Object.entries(params)) {
      pythonLines.push(`        "${k}": "${v}",`);
    }
    pythonLines.push(`    }`);
  }
  pythonLines.push(`)`);
  pythonLines.push(`data = response.json()`);
  pythonLines.push(`print(data)`);

  const rLines = [`library(httr)`, `library(jsonlite)`, ``];
  if (Object.keys(params).length > 0) {
    rLines.push(`response <- GET(`);
    rLines.push(`  "${base}${path}",`);
    rLines.push(`  query = list(`);
    const entries = Object.entries(params);
    entries.forEach(([k, v], i) => {
      rLines.push(`    ${k} = "${v}"${i < entries.length - 1 ? "," : ""}`);
    });
    rLines.push(`  )`);
    rLines.push(`)`);
  } else {
    rLines.push(`response <- GET("${base}${path}")`);
  }
  rLines.push(`data <- fromJSON(content(response, "text", encoding = "UTF-8"))`);
  rLines.push(`print(data)`);

  return {
    cURL: { source: curl, lang: "curl" },
    Python: { source: pythonLines.join("\n"), lang: "python" },
    R: { source: rLines.join("\n"), lang: "r" },
  };
}
