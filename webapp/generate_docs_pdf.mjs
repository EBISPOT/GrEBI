#!/usr/bin/env node
/**
 * GrEBI Documentation PDF Generator
 *
 * Reads docs/_sidebar.yaml for page ordering, processes all markdown pages,
 * executes API and query-template examples against a running stack to capture
 * live output, and generates a single PDF via pandoc.
 *
 * Shares api2code.mjs with the React UI for consistent code snippets.
 *
 * Usage:
 *   node generate_docs_pdf.mjs [--api-url URL] [--docs-dir DIR] [--output FILE]
 */

import fs from "fs";
import path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import { createRequire } from "module";
import api2code from "./api2code.mjs";
import query2code from "./query2code.mjs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Resolve js-yaml: try grebi_ui/node_modules first (local dev), then cwd
const require = createRequire(
  pathToFileURL(
    fs.existsSync(path.join(__dirname, "grebi_ui", "node_modules"))
      ? path.join(__dirname, "grebi_ui", "package.json")
      : path.join(process.cwd(), "package.json")
  )
);
const yaml = require("js-yaml");

// ── Include resolution ───────────────────────────────────────────────

// Resolve <include src="path/to/file.md" /> tags recursively.
function resolveIncludes(md, baseDir, seen = new Set()) {
  return md.replace(/^<include\s+src=["']([^"']+)["']\s*\/>$/gm, (_match, relPath) => {
    const absPath = path.resolve(baseDir, relPath);
    if (seen.has(absPath)) return '';
    if (!fs.existsSync(absPath)) {
      console.warn(`  Warning: included file not found: ${relPath}`);
      return '';
    }
    seen.add(absPath);
    const content = fs.readFileSync(absPath, 'utf8');
    return resolveIncludes(content, path.dirname(absPath), seen);
  });
}

// ── CLI args ─────────────────────────────────────────────────────────
function parseArgs() {
  const args = process.argv.slice(2);
  const opts = {
    apiUrl: "http://localhost:8090",
    docsDir: process.env.GREBI_DOCS_PATH || "/opt/docs",
    output: "grebi-docs.html",
  };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--api-url" && args[i + 1]) opts.apiUrl = args[++i];
    else if (args[i] === "--docs-dir" && args[i + 1]) opts.docsDir = args[++i];
    else if (args[i] === "--output" && args[i + 1]) opts.output = args[++i];
  }
  return opts;
}

// ── Sidebar helpers ──────────────────────────────────────────────────
function normalizeSidebar(entries) {
  return entries.map(entry => {
    if (typeof entry === 'string') return { path: entry };
    if (entry.file) {
      const norm = { path: entry.file };
      if (entry.children) norm.children = normalizeSidebar(entry.children);
      return norm;
    }
    if (entry.children) entry.children = normalizeSidebar(entry.children);
    return entry;
  });
}
function collectPages(sidebar) {
  const pages = [];
  for (const entry of sidebar) {
    if (entry.path) pages.push(entry.path);
    if (entry.children) pages.push(...collectPages(entry.children));
  }
  return pages;
}

// ── API helpers ──────────────────────────────────────────────────────
async function checkApi(apiUrl) {
  try {
    const r = await fetch(`${apiUrl}/api/health`, { signal: AbortSignal.timeout(5000) });
    return r.ok;
  } catch {
    return false;
  }
}

async function getAvailableGraphs(apiUrl) {
  try {
    const r = await fetch(`${apiUrl}/api/v1/graphs`, { signal: AbortSignal.timeout(10000) });
    if (!r.ok) return [];
    return await r.json();
  } catch {
    return [];
  }
}

async function fetchApiExample(apiUrl, urlPath, params) {
  try {
    const qs = new URLSearchParams(params).toString();
    const fullUrl = `${apiUrl}${urlPath}${qs ? "?" + qs : ""}`;
    const r = await fetch(fullUrl, { signal: AbortSignal.timeout(30000) });
    if (!r.ok) return null;
    const data = JSON.stringify(await r.json(), null, 2);
    const lines = data.split("\n");
    return lines.length > 30
      ? lines.slice(0, 30).join("\n") + "\n... (truncated)"
      : data;
  } catch {
    return null;
  }
}

// ── HTML attribute extractor ─────────────────────────────────────────
function extractAttr(tag, attr) {
  const m = tag.match(new RegExp(`${attr}=["']([^"']*)["']`));
  return m ? m[1] : null;
}

function extractAllAttrs(tag) {
  const attrs = {};
  const re = /(\w+)=["']([^"']*)["']/g;
  let m;
  while ((m = re.exec(tag)) !== null) {
    attrs[m[1]] = m[2];
  }
  return attrs;
}

function parseParams(paramsStr) {
  if (!paramsStr) return {};
  try {
    return JSON.parse(paramsStr);
  } catch {
    return {};
  }
}

// ── Markdown processors ──────────────────────────────────────────────

function escapeHtml(s) {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function snippetsToHtmlTable(snippets) {
  const rows = [];
  for (const [title, { source, lang }] of Object.entries(snippets)) {
    const prismLang = lang === "curl" ? "bash" : lang;
    rows.push(
      `<tr><td class="snippet-lang">${escapeHtml(title)}</td>` +
      `<td class="snippet-code"><pre><code class="language-${prismLang}">${escapeHtml(source)}</code></pre></td></tr>`
    );
  }
  return `<table class="snippet-table"><tbody>${rows.join("")}</tbody></table>`;
}

function codeSnippetsToMarkdown(method, apiUrl, urlPath, params) {
  const snippets = api2code(method, apiUrl, urlPath, params);
  return snippetsToHtmlTable(snippets);
}

function querySnippetsToMarkdown(apiUrl, graph, queryId, params) {
  const paramIds = Object.keys(params);
  const snippets = query2code(apiUrl, graph, queryId, paramIds, params);
  return snippetsToHtmlTable(snippets);
}

async function processApiExamples(content, apiUrl, apiAvailable) {
  const regex = /<api-example\s[^>]*\/\s*>/g;
  const matches = [...content.matchAll(regex)];
  for (const match of matches) {
    const tag = match[0];
    const attrs = extractAllAttrs(tag);
    const method = attrs.method || "GET";
    const url = attrs.url || "";
    const params = {};
    for (const [k, v] of Object.entries(attrs)) {
      if (k !== "method" && k !== "url") params[k] = v;
    }

    const codeBlock = codeSnippetsToMarkdown(method, apiUrl, url, params);

    let outputBlock = "";
    if (apiAvailable) {
      const data = await fetchApiExample(apiUrl, url, params);
      if (data) {
        outputBlock = `\n\n**Response:**\n\n\`\`\`json\n${data}\n\`\`\``;
      } else {
        outputBlock = "\n\n*Could not capture response.*";
      }
    }

    content = content.replace(tag, `${codeBlock}${outputBlock}`);
  }
  return content;
}

// The <openapi-reference /> tag: the API reference, from the description the
// running stack serves, as headings and tables (the UI renders the same
// description live, with a Try it block per example).
function openApiToMarkdown(spec) {
  const resolve = (o) => {
    if (o && o.$ref) {
      let found = spec;
      for (const step of String(o.$ref).replace(/^#\//, "").split("/")) found = found?.[step];
      return found ?? o;
    }
    return o;
  };
  const label = (schema) => {
    if (!schema) return "";
    if (schema.$ref) return String(schema.$ref).split("/").pop();
    if (schema.type === "array") return `array of ${label(schema.items) || "any"}`;
    if (schema.enum) return schema.enum.join(" \\| ");
    return schema.type || "";
  };
  const cell = (s) => String(s ?? "").replace(/\|/g, "\\|").replace(/\s*\n\s*/g, " ");
  const operations = [];
  for (const [p, item] of Object.entries(spec.paths || {})) {
    for (const method of ["get", "post", "put", "delete", "patch"]) {
      if (item[method]) operations.push({ method, path: p, item, op: item[method] });
    }
  }
  const tags = (spec.tags || []).map((t) => t.name);
  for (const o of operations) for (const t of o.op.tags || []) if (!tags.includes(t)) tags.push(t);
  const out = [];
  if (spec.info?.description) out.push(spec.info.description.trim(), "");
  for (const tag of tags) {
    const ops = operations.filter((o) => (o.op.tags || []).includes(tag));
    if (ops.length === 0) continue;
    out.push(`## ${tag}`, "");
    const description = (spec.tags || []).find((t) => t.name === tag)?.description;
    if (description) out.push(description, "");
    for (const { method, path: p, item, op } of ops) {
      out.push(`#### ${method.toUpperCase()} \`${p}\`${op.deprecated ? " (deprecated)" : ""}`, "", `**${op.summary || ""}**`, "");
      if (op.description) out.push(op.description.trim(), "");
      const parameters = [...(item.parameters || []), ...(op.parameters || [])].map(resolve);
      if (parameters.length > 0) {
        out.push("| Parameter | In | Type | Description |", "|---|---|---|---|");
        for (const q of parameters) {
          const notes = [q.description || ""];
          if (q.schema?.default !== undefined) notes.push(`Default \`${q.schema.default}\`.`);
          if (q.example !== undefined) notes.push(`Example \`${Array.isArray(q.example) ? q.example.join(", ") : q.example}\`.`);
          out.push(`| \`${cell(q.name)}\`${q.required ? " (required)" : ""} | ${q.in} | ${cell(label(q.schema))} | ${cell(notes.join(" "))} |`);
        }
        out.push("");
      }
      if (op.requestBody) {
        const body = resolve(op.requestBody);
        const type = Object.keys(body.content || {})[0];
        out.push(`Request body (${type}${body.required ? ", required" : ""})${body.description ? ": " + body.description : ""}`, "");
        const example = type && body.content[type].example;
        if (example !== undefined) out.push("```json", typeof example === "string" ? example : JSON.stringify(example, null, 2), "```", "");
      }
      for (const [status, r] of Object.entries(op.responses || {})) {
        const response = resolve(r);
        const type = response.content ? Object.keys(response.content)[0] : null;
        out.push(`- \`${status}\` ${response.description || ""}${type && type !== "application/json" ? ` (${type})` : ""}`);
      }
      out.push("");
    }
  }
  return out.join("\n");
}

// The <mcp-reference /> tag: what the MCP server publishes, from the catalogue
// the running stack serves, with the template tools under each graph they are
// for (every graph has its own query templates).
function mcpCatalogueToMarkdown(c) {
  const controls = ["graph", "sortBy", "sortDir", "pageNum", "pageSize"];
  const typeOf = (s) => {
    if (!s) return "";
    if (s.enum) return s.enum.join(" \\| ");
    if (Array.isArray(s.type)) return s.type.join(" \\| ");
    if (s.type === "array") return `array of ${typeOf(s.items) || "any"}`;
    return s.type || "";
  };
  const cell = (s) => String(s ?? "").replace(/\|/g, "\\|").replace(/\s*\n\s*/g, " ");
  const argumentsTable = (tool, only) => {
    const props = tool.inputSchema?.properties || {};
    const required = tool.inputSchema?.required || [];
    const names = Object.keys(props).filter((n) => !only || only(n));
    if (names.length === 0) return ["No arguments of its own.", ""];
    const rows = ["| Argument | Type | Description |", "|---|---|---|"];
    for (const n of names) {
      rows.push(`| \`${cell(n)}\`${required.includes(n) ? " (required)" : ""} | ${cell(typeOf(props[n]))} | ${cell(props[n]?.description || "")} |`);
    }
    return [...rows, ""];
  };
  const out = [];
  out.push(`Server \`${c.server.name}\` ${c.server.version}, endpoint \`${c.server.endpoint}\` (${c.server.transport}). The instance these docs were built against serves ${c.graphs.map((g) => `\`${g}\``).join(", ")}; a running instance's own \`tools/list\` is authoritative for what it serves.`, "");
  out.push("## Instructions to agents", "", ...c.instructions.split(/\n\s*\n/).map((p) => "> " + p.replace(/\s*\n\s*/g, " ")), "");
  out.push("## Resources", "", "| URI | Name | Type |", "|---|---|---|");
  for (const r of c.resources) out.push(`| \`${r.uri}\` | ${cell(r.name)}${r.description ? ": " + cell(r.description) : ""} | ${r.mimeType || ""} |`);
  out.push("");
  out.push("## Tools for every graph", "");
  for (const t of c.tools.filter((t) => !t.template)) {
    out.push(`#### \`${t.name}\``, "", t.description || "", "", ...argumentsTable(t));
    const outputs = Object.keys(t.outputSchema?.properties || {});
    if (outputs.length > 0) out.push(`Returns ${outputs.map((o) => `\`${o}\``).join(", ")}.`, "");
  }
  for (const graph of c.graphs) {
    out.push(`## Tools for \`${graph}\``, "");
    const tools = c.tools.filter((t) => t.template && (!t.graphs || t.graphs.includes(graph)));
    if (tools.length === 0) out.push("No query templates for this graph.", "");
    for (const t of tools) {
      out.push(`#### \`${t.name}\`${t.graphs ? "" : " (runs on any graph the instance serves)"}`, "", t.description || "", "", ...argumentsTable(t, (n) => !controls.includes(n)));
      const sortBy = t.inputSchema?.properties?.sortBy?.enum || [];
      if (sortBy.length > 0) out.push(`Sort by ${sortBy.map((s) => `\`${s}\``).join(", ")}.`, "");
      const columns = Object.entries(t.outputSchema?.properties?.rows?.items?.properties || {});
      if (columns.length > 0) out.push(`Result columns: ${columns.map(([n, s]) => `\`${n}\`${typeOf(s) ? ` (${typeOf(s)})` : ""}`).join(", ")}.`, "");
    }
  }
  const others = c.tools.filter((t) => t.template && t.graphs && !t.graphs.some((g) => c.graphs.includes(g)));
  if (others.length > 0) {
    out.push("## Templates for other graphs", "",
      "Query templates in this version of GrEBI for graphs the instance these docs were built against does not serve. An instance serving one of these graphs offers them as tools, with the same arguments as above.", "",
      "| Template | Question | Graphs |", "|---|---|---|");
    for (const t of others) {
      const description = t.description || "";
      const colon = description.indexOf(": ");
      out.push(`| \`${t.name}\` | ${cell(colon > 0 ? description.slice(0, colon) : description)} | ${t.graphs.map((g) => `\`${g}\``).join(", ")} |`);
    }
    out.push("");
  }
  return out.join("\n");
}

async function processMcpReference(content, apiUrl, apiAvailable) {
  const regex = /<mcp-reference\s*\/\s*>/g;
  if (!regex.test(content)) return content;
  let markdown;
  if (!apiAvailable) {
    markdown = "*The MCP reference could not be captured: the API was not available when these docs were built.*";
  } else {
    try {
      const r = await fetch(`${apiUrl}/api/v1/mcp/catalogue`, { signal: AbortSignal.timeout(10000) });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      markdown = mcpCatalogueToMarkdown(await r.json());
      console.log("  Rendered the MCP reference from the server's catalogue");
    } catch (e) {
      console.warn(`  Could not fetch the MCP catalogue: ${e.message}`);
      markdown = `*The MCP reference could not be captured: ${e.message}.*`;
    }
  }
  return content.replace(/<mcp-reference\s*\/\s*>/g, markdown);
}

async function processOpenApiReference(content, apiUrl, apiAvailable) {
  const regex = /<openapi-reference\s*\/\s*>/g;
  if (!regex.test(content)) return content;
  let markdown;
  if (!apiAvailable) {
    markdown = "*The API reference could not be captured: the API was not available when these docs were built.*";
  } else {
    try {
      const r = await fetch(`${apiUrl}/api/v1/openapi.json`, { signal: AbortSignal.timeout(10000) });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      markdown = openApiToMarkdown(await r.json());
      console.log("  Rendered the API reference from the OpenAPI description");
    } catch (e) {
      console.warn(`  Could not fetch the OpenAPI description: ${e.message}`);
      markdown = `*The API reference could not be captured: ${e.message}.*`;
    }
  }
  return content.replace(/<openapi-reference\s*\/\s*>/g, markdown);
}

async function processQueryTemplateExamples(
  content,
  apiUrl,
  apiAvailable,
  availableGraphs
) {
  const regex = /<query-template\s[^>]*\/\s*>/g;
  const matches = [...content.matchAll(regex)];
  for (const match of matches) {
    const tag = match[0];
    const attrs = extractAllAttrs(tag);
    const queryId = attrs.id || "";
    const graph = attrs.graph || "";
    const params = {};
    for (const [k, v] of Object.entries(attrs)) {
      if (k !== "id" && k !== "graph") params[k] = v;
    }

    const codeBlock = querySnippetsToMarkdown(apiUrl, graph, queryId, params);

    let outputBlock = "";
    if (apiAvailable && availableGraphs.includes(graph)) {
      const queryUrl = `/api/v1/graphs/${graph}/query/${queryId}`;
      const data = await fetchApiExample(apiUrl, queryUrl, params);
      if (data) {
        outputBlock = `\n\n**Response:**\n\n\`\`\`json\n${data}\n\`\`\``;
      } else {
        outputBlock = "\n\n*Could not capture response.*";
      }
    } else if (!apiAvailable) {
      outputBlock =
        "\n\n*API not available — run with a live stack to capture output.*";
    } else {
      outputBlock = `\n\n*Graph \`${graph}\` not loaded — skipping live example.*`;
    }

    const header = `**Query template:** \`${queryId}\` on graph \`${graph}\`\n\n`;
    content = content.replace(tag, `${header}${codeBlock}${outputBlock}`);
  }
  return content;
}

function processLinks(content) {
  return content.replace(
    /\[([^\]]+)\]\(\.\/?[a-z0-9_-]+\.md\)/g,
    (_, text) => `<strong>${text}</strong>`
  );
}

function resolveImages(content, docsDir) {
  return content.replace(
    /!\[([^\]]*)\]\(\.\/?images\/([^)]+)\)/g,
    (original, alt, filename) => {
      const absPath = path.join(docsDir, "images", filename);
      if (!fs.existsSync(absPath)) return original;
      const ext = path.extname(filename).toLowerCase();
      const mimeTypes = { ".svg": "image/svg+xml", ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".gif": "image/gif", ".webp": "image/webp" };
      const mime = mimeTypes[ext] || "application/octet-stream";
      const b64 = fs.readFileSync(absPath).toString("base64");
      return `![${alt}](data:${mime};base64,${b64})`;
    }
  );
}

// ── PubMed reference tags ─────────────────────────────────────────────
function formatCitation(entry) {
  let cite = "";
  if (entry.authors) cite += `${entry.authors}. `;
  if (entry.title) cite += `${entry.title} `;
  if (entry.journal) cite += `<em>${entry.journal}</em>`;
  if (entry.year) cite += ` (${entry.year})`;
  if (entry.volume) {
    cite += ` ${entry.volume}`;
    if (entry.issue) cite += `(${entry.issue})`;
  }
  if (entry.pages) cite += `: ${entry.pages}`;
  cite += ".";
  if (entry.doi) cite += ` doi: ${entry.doi}`;
  return cite;
}

// Phase 1 (markdown): replace <pubmed> tags with placeholders that survive marked.parse()
function processPubmedInline(content, docsDir) {
  const cachePath = path.join(docsDir, "pubmed_cache.json");
  let cache = {};
  if (fs.existsSync(cachePath)) {
    cache = JSON.parse(fs.readFileSync(cachePath, "utf8"));
  }
  const transformed = content.replace(
    /<pubmed\s+id="(\d+)"\s*\/>/g,
    (_, pmid) => `<span class="pm-ref" data-pmid="${pmid}"></span>`
  );
  return { content: transformed, cache };
}

// Phase 2 (HTML): single pass — number pubmed placeholders and external links in document order
function convertReferences(html, pubmedCache) {
  const refList = [];       // ordered list of { type, key, num }
  const keyToNum = new Map();
  let counter = 0;

  // Match both pubmed placeholders and external links in document order
  const combined = /<span class="pm-ref" data-pmid="(\d+)"><\/span>|<a\s+href="(https?:\/\/[^"]+)"[^>]*>(.*?)<\/a>/gi;
  const transformed = html.replace(combined, (match, pmid, url, linkText) => {
    if (pmid !== undefined) {
      // pubmed placeholder
      const key = `pm:${pmid}`;
      if (!keyToNum.has(key)) {
        const n = ++counter;
        keyToNum.set(key, n);
        refList.push({ type: "pubmed", pmid, num: n });
      }
      const n = keyToNum.get(key);
      return `<a href="#ref-${n}" class="fn-ref"><sup>[${n}]</sup></a>`;
    } else {
      // external link
      const key = `url:${url}`;
      if (!keyToNum.has(key)) {
        const n = ++counter;
        keyToNum.set(key, n);
        refList.push({ type: "link", url, num: n });
      }
      const n = keyToNum.get(key);
      return `${linkText}<a href="#ref-${n}" class="fn-ref"><sup>[${n}]</sup></a>`;
    }
  });

  if (refList.length === 0) return html;

  const items = refList.map(r => {
    if (r.type === "pubmed") {
      const entry = pubmedCache[r.pmid];
      const text = entry ? formatCitation(entry) : `PMID: ${r.pmid}`;
      return `<li id="ref-${r.num}"><span class="fn-num">[${r.num}]</span> <a href="https://pubmed.ncbi.nlm.nih.gov/${r.pmid}/" style="color:inherit;text-decoration:none;">${text}</a></li>`;
    } else {
      return `<li id="ref-${r.num}"><span class="fn-num">[${r.num}]</span> <a href="${r.url}">${r.url}</a></li>`;
    }
  }).join("\n");

  const section = `<section class="footnotes"><h2>References</h2><ol>${items}</ol></section>`;
  return transformed.replace(/<\/body>/, `${section}</body>`);
}

// ── Main ─────────────────────────────────────────────────────────────
async function main() {
  const opts = parseArgs();

  if (!fs.existsSync(opts.docsDir)) {
    console.error(`ERROR: docs directory not found: ${opts.docsDir}`);
    process.exit(1);
  }

  const indexPath = path.join(opts.docsDir, "index.md");
  if (!fs.existsSync(indexPath)) {
    console.error(`ERROR: index.md not found in ${opts.docsDir}`);
    process.exit(1);
  }

  const apiAvailable = await checkApi(opts.apiUrl);
  let availableGraphs = [];
  if (apiAvailable) {
    console.log(`API available at ${opts.apiUrl}`);
    availableGraphs = await getAvailableGraphs(opts.apiUrl);
    console.log(`Available graphs: ${availableGraphs.join(", ")}`);
  } else {
    console.log(
      `API not available at ${opts.apiUrl} — examples will be static`
    );
  }

  console.log(`  Processing: index.md`);
  let content = resolveIncludes(fs.readFileSync(indexPath, "utf8"), opts.docsDir);
  content = await processApiExamples(content, opts.apiUrl, apiAvailable);
  content = await processQueryTemplateExamples(
    content,
    opts.apiUrl,
    apiAvailable,
    availableGraphs
  );
  content = await processOpenApiReference(content, opts.apiUrl, apiAvailable);
  content = await processMcpReference(content, opts.apiUrl, apiAvailable);
  const pubmed = processPubmedInline(content, opts.docsDir);
  content = pubmed.content;
  content = processLinks(content);
  content = resolveImages(content, opts.docsDir);

  const fullMd = content;
  console.log(`\nRendering documentation HTML`);

  const { marked } = await import(require.resolve("marked"));

  // Load PrismJS for syntax highlighting (same as the frontend)
  const Prism = require("prismjs");
  const loadLanguages = require("prismjs/components/");
  loadLanguages(["python", "bash", "r", "cypher", "json", "yaml"]);

  const prismThemeCss = fs.readFileSync(
    require.resolve("prismjs/themes/prism.css"), "utf8"
  );

  // Highlight code blocks after marked renders them
  function highlightCodeBlocks(html) {
    return html.replace(
      /<pre><code class="language-(\w+)">([\s\S]*?)<\/code><\/pre>/gi,
      (_, lang, code) => {
        const prismLang = lang === "curl" ? "bash" : lang;
        const grammar = Prism.languages[prismLang];
        if (!grammar) return `<pre><code>${code}</code></pre>`;
        // Decode HTML entities that marked escaped
        // Important: decode '&amp;' last to avoid double-unescaping (e.g. '&amp;lt;' -> '&lt;', not '<')
        const decoded = code
          .replace(/&lt;/g, "<")
          .replace(/&gt;/g, ">")
          .replace(/&quot;/g, '"')
          .replace(/&#39;/g, "'")
          .replace(/&amp;/g, "&");
        const highlighted = Prism.highlight(decoded, grammar, prismLang);
        return `<pre class="language-${prismLang}"><code class="language-${prismLang}">${highlighted}</code></pre>`;
      }
    );
  }

  // Strip HTML tags iteratively so nested-like sequences (e.g. "<scr<script>ipt>")
  // can't reintroduce a tag after a single pass.
  function stripTags(s) {
    let prev;
    do { prev = s; s = s.replace(/<[^>]+>/g, ""); } while (s !== prev);
    return s;
  }

  // Add IDs to headings (marked doesn't generate them by default)
  let headingCounter = 0;
  const rawHtml = highlightCodeBlocks(marked.parse(fullMd));
  const withIds = rawHtml.replace(
    /<h([1-6])>(.*?)<\/h[1-6]>/gi,
    (_, level, text) => {
      const id = `h-${headingCounter++}-${stripTags(text).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "")}`;
      return `<h${level} id="${id}">${text}</h${level}>`;
    }
  );

  // Build a TOC from h1/h2/h3 and number the headings
  const tocEntries = [];
  withIds.replace(/<h([123])\s+id="([^"]*)"[^>]*>(.*?)<\/h[123]>/gi, (_, level, id, text) => {
    tocEntries.push({ level: parseInt(level), id, text: stripTags(text) });
  });

  // Assign section numbers (1, 1.1, 1.1.1, etc.)
  const counters = [0, 0, 0];
  for (const entry of tocEntries) {
    const idx = entry.level - 1;
    counters[idx]++;
    for (let i = idx + 1; i < counters.length; i++) counters[i] = 0;
    entry.number = counters.slice(0, idx + 1).join(".");
  }

  // Inject numbers into headings in the body HTML
  let numberedHtml = withIds;
  for (const entry of tocEntries) {
    numberedHtml = numberedHtml.replace(
      new RegExp(`(<h${entry.level}\\s+id="${entry.id}"[^>]*>)`),
      `$1${entry.number} `
    );
  }

  const tocHtml = tocEntries.length > 0
    ? `<nav class="toc"><h2>Contents</h2><ul>${tocEntries.map(e =>
        `<li class="toc-h${e.level}"><a href="#${e.id}"><span class="toc-num">${e.number}</span> ${e.text}</a></li>`
      ).join("")}</ul></nav>`
    : "";

  // Inline logo for cover page
  let coverLogoTag = "";
  const logoPath = path.join(opts.docsDir, "images", "logo.svg");
  if (fs.existsSync(logoPath)) {
    const logoB64 = fs.readFileSync(logoPath).toString("base64");
    coverLogoTag = `<img src="data:image/svg+xml;base64,${logoB64}" alt="GrEBI Logo">`;
  }

  const html = `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  @import url('https://fonts.googleapis.com/css2?family=Source+Serif+4:ital,opsz,wght@0,8..60,300..900;1,8..60,300..900&display=swap');
  ${prismThemeCss}
  @page { margin: 0.75in; size: A4; }
  body { font-family: "Source Serif 4", Palatino, Georgia, serif;
         font-size: 11pt; line-height: 1.5; color: #1a1a1a; max-width: 100%; }
  h1 { font-size: 16pt; border-bottom: 2px solid #734595; padding-bottom: 4px; margin-top: 1.5em; page-break-after: avoid; }
  h2 { font-size: 13pt; border-bottom: 1px solid #734595; padding-bottom: 3px; margin-top: 1.2em; page-break-after: avoid; }
  h3 { font-size: 11pt; margin-top: 1em; page-break-after: avoid; }
  code { font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
         background: #f3f4f6; padding: 1px 3px; border-radius: 2px; font-size: 0.85em; }
  pre { background: #f8f9fa; color: #1a1a1a; padding: 10px 14px; border-radius: 5px; border: 1px solid #d1d5db;
        overflow-x: auto; font-size: 8pt !important; line-height: 1.4; page-break-inside: avoid; }
  pre code { background: none; color: inherit; padding: 0; font-size: 8pt !important; }
  pre[class*="language-"] { font-size: 8pt !important; background: #f8f9fa; }
  code[class*="language-"] { font-size: 8pt !important; }
  hr { border: none; border-top: 1px solid #d1d5db; margin: 2em 0; }
  table { border-collapse: collapse; width: 100%; margin: 1em 0; page-break-inside: avoid; }
  th, td { border: 1px solid #d1d5db; padding: 4px 8px; text-align: left; font-size: 8pt; }
  th { background: #f9fafb; font-weight: 600; }
  .snippet-table { border: 1px solid #d1d5db; border-radius: 5px; overflow: hidden; }
  .snippet-table td { border: none; border-bottom: 1px solid #d1d5db; padding: 0; vertical-align: top; }
  .snippet-table tr:last-child td { border-bottom: none; }
  .snippet-table .snippet-lang { background: #e5e7eb; color: #1a1a1a; font-size: 7.5pt; font-weight: 600;
    padding: 6px 10px; width: 4em; white-space: nowrap; font-family: -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }
  .snippet-table .snippet-code { padding: 0; }
  .snippet-table pre { margin: 0; border-radius: 0; }
  blockquote { border-left: 4px solid #734595; margin: 1em 0; padding: 0.5em 1em;
               background: #f5f0f8; color: #4a2d63; }
  a { color: #734595; }
  .cover { text-align: center; padding: 1in 0 1.5in; page-break-after: always; }
  .cover img { max-width: 280px; margin-bottom: 1.5em; }
  .cover h1 { border: none; font-size: 24pt; color: #1a1a1a; }
  .cover p { font-size: 11pt; color: #6b7280; }
  img { max-width: 100%; height: auto; }
  .toc { page-break-after: always; }
  .toc h2 { border-bottom: 2px solid #734595; margin-bottom: 1em; }
  .toc ul { list-style: none; padding: 0; margin: 0; }
  .toc li { display: flex; align-items: baseline; padding: 4px 0;
            border-bottom: 1px dotted #d1d5db; }
  .toc li:last-child { border-bottom: none; }
  .toc .toc-h1 { font-weight: 600; font-size: 9pt; margin-top: 0.5em; }
  .toc .toc-h2 { padding-left: 1.5em; font-size: 8.5pt; }
  .toc .toc-h3 { padding-left: 3em; font-size: 8pt; color: #4b5563; }
  .toc a { text-decoration: none; color: #1a1a1a; flex: 1; }
  .toc a:hover { color: #2563eb; }
  .toc .toc-num { display: inline-block; min-width: 2em; color: #6b7280; }
  .fn-ref { font-size: 0.75em; color: #734595; text-decoration: none; }
  .fn-ref:hover { text-decoration: underline; }
  .footnotes { margin-top: 2em; border-top: 2px solid #734595; padding-top: 1em; page-break-before: always; }
  .footnotes h2 { font-size: 13pt; border-bottom: 1px solid #734595; padding-bottom: 3px; }
  .footnotes ol { list-style: none; padding: 0; margin: 0; }
  .footnotes li { font-size: 7.5pt; padding: 2px 0; word-break: break-all; }
  .footnotes .fn-num { display: inline-block; min-width: 2em; font-weight: 600; color: #4b5563; }
  .footnotes a { color: #734595; text-decoration: none; }
</style>
</head>
<body>
  <div class="cover">
    ${coverLogoTag}
    <h1>GrEBI (Graphs@EBI)</h1>
    <p>Samples, Phenotypes, and Ontologies Team (SPOT)</p>
    <p>EMBL-EBI</p>
    <p style="margin-top:2em;font-size:9pt;color:#9ca3af;">Updated ${new Date().toLocaleDateString("en-GB", { day: "numeric", month: "long", year: "numeric" })}</p>
  </div>
  ${tocHtml}
  ${numberedHtml}
</body>
</html>`;

  const finalHtml = convertReferences(html, pubmed.cache);

  // The HTML embeds all images/CSS as data URIs, so it is fully self-contained.
  // PDF rendering is performed separately by render_pdf.mjs (in the upstream
  // puppeteer image), keeping chromium out of the GrEBI images.
  fs.writeFileSync(opts.output, finalHtml);
  console.log(`HTML written: ${opts.output}`);
}

main();
