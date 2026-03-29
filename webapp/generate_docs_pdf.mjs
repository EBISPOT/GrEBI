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

// ── CLI args ─────────────────────────────────────────────────────────
function parseArgs() {
  const args = process.argv.slice(2);
  const opts = {
    apiUrl: "http://localhost:8090",
    docsDir: process.env.GREBI_DOCS_PATH || "/opt/docs",
    output: "grebi-docs.pdf",
  };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--api-url" && args[i + 1]) opts.apiUrl = args[++i];
    else if (args[i] === "--docs-dir" && args[i + 1]) opts.docsDir = args[++i];
    else if (args[i] === "--output" && args[i + 1]) opts.output = args[++i];
  }
  return opts;
}

// ── Sidebar helpers ──────────────────────────────────────────────────
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

function parseParams(paramsStr) {
  if (!paramsStr) return {};
  try {
    return JSON.parse(paramsStr);
  } catch {
    return {};
  }
}

// ── Markdown processors ──────────────────────────────────────────────

function codeSnippetsToMarkdown(method, apiUrl, urlPath, params) {
  const snippets = api2code(method, apiUrl, urlPath, params);
  const blocks = [];
  for (const [title, { source, lang }] of Object.entries(snippets)) {
    const prismLang = lang === "curl" ? "bash" : lang;
    blocks.push(`**${title}:**\n\n\`\`\`${prismLang}\n${source}\n\`\`\``);
  }
  return blocks.join("\n\n");
}

function querySnippetsToMarkdown(apiUrl, graph, queryId, params) {
  const paramIds = Object.keys(params);
  const snippets = query2code(apiUrl, graph, queryId, paramIds, params);
  const blocks = [];
  for (const [title, { source, lang }] of Object.entries(snippets)) {
    blocks.push(`**${title}:**\n\n\`\`\`${lang}\n${source}\n\`\`\``);
  }
  return blocks.join("\n\n");
}

async function processApiExamples(content, apiUrl, apiAvailable) {
  const regex = /<api-example\s[^>]*\/\s*>/g;
  const matches = [...content.matchAll(regex)];
  for (const match of matches) {
    const tag = match[0];
    const method = extractAttr(tag, "method") || "GET";
    const url = extractAttr(tag, "url") || "";
    const params = parseParams(extractAttr(tag, "params"));

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
    const queryId = extractAttr(tag, "id") || "";
    const graph = extractAttr(tag, "graph") || "";
    const params = parseParams(extractAttr(tag, "params"));

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

// ── Main ─────────────────────────────────────────────────────────────
async function main() {
  const opts = parseArgs();

  if (!fs.existsSync(opts.docsDir)) {
    console.error(`ERROR: docs directory not found: ${opts.docsDir}`);
    process.exit(1);
  }

  const sidebarPath = path.join(opts.docsDir, "_sidebar.yaml");
  if (!fs.existsSync(sidebarPath)) {
    console.error(`ERROR: _sidebar.yaml not found in ${opts.docsDir}`);
    process.exit(1);
  }

  const sidebar = yaml.load(fs.readFileSync(sidebarPath, "utf8"));
  const orderedPages = collectPages(sidebar);

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

  const sections = [];
  for (const pagePath of orderedPages) {
    const mdFile = path.join(opts.docsDir, pagePath);
    if (!fs.existsSync(mdFile)) {
      console.log(`  Warning: ${pagePath} not found, skipping`);
      continue;
    }
    console.log(`  Processing: ${pagePath}`);
    let content = fs.readFileSync(mdFile, "utf8");
    content = await processApiExamples(content, opts.apiUrl, apiAvailable);
    content = await processQueryTemplateExamples(
      content,
      opts.apiUrl,
      apiAvailable,
      availableGraphs
    );
    content = processLinks(content);
    content = resolveImages(content, opts.docsDir);
    sections.push(content);
  }

  const fullMd = sections.join("\n\n---\n\n");
  console.log(`\nGenerating PDF: ${opts.output}`);

  const { marked } = await import(require.resolve("marked"));
  const puppeteer = (await import(require.resolve("puppeteer"))).default;

  // Load PrismJS for syntax highlighting (same as the frontend)
  const Prism = require("prismjs");
  const loadLanguages = require("prismjs/components/");
  loadLanguages(["python", "bash", "r", "cypher", "json", "yaml"]);

  const prismThemeCss = fs.readFileSync(
    require.resolve("prismjs/themes/prism-tomorrow.css"), "utf8"
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
        const decoded = code
          .replace(/&amp;/g, "&").replace(/&lt;/g, "<")
          .replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/&#39;/g, "'");
        const highlighted = Prism.highlight(decoded, grammar, prismLang);
        return `<pre class="language-${prismLang}"><code class="language-${prismLang}">${highlighted}</code></pre>`;
      }
    );
  }

  // Add IDs to headings (marked doesn't generate them by default)
  let headingCounter = 0;
  const rawHtml = highlightCodeBlocks(marked.parse(fullMd));
  const withIds = rawHtml.replace(
    /<h([1-6])>(.*?)<\/h[1-6]>/gi,
    (_, level, text) => {
      const id = `h-${headingCounter++}-${text.replace(/<[^>]+>/g, "").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "")}`;
      return `<h${level} id="${id}">${text}</h${level}>`;
    }
  );

  // Build a TOC from h1/h2/h3 and number the headings
  const tocEntries = [];
  withIds.replace(/<h([123])\s+id="([^"]*)"[^>]*>(.*?)<\/h[123]>/gi, (_, level, id, text) => {
    tocEntries.push({ level: parseInt(level), id, text: text.replace(/<[^>]+>/g, "") });
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
        `<li class="toc-h${e.level}"><a href="#${e.id}"><span class="toc-num">${e.number}</span>${e.text}</a></li>`
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
  ${prismThemeCss}
  @page { margin: 0.75in; size: A4; }
  body { font-family: -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
         font-size: 9pt; line-height: 1.5; color: #1a1a1a; max-width: 100%; }
  h1 { font-size: 16pt; border-bottom: 2px solid #734595; padding-bottom: 4px; margin-top: 1.5em; page-break-after: avoid; }
  h2 { font-size: 13pt; border-bottom: 1px solid #734595; padding-bottom: 3px; margin-top: 1.2em; page-break-after: avoid; }
  h3 { font-size: 11pt; margin-top: 1em; page-break-after: avoid; }
  code { font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
         background: #f3f4f6; padding: 1px 3px; border-radius: 2px; font-size: 0.85em; }
  pre { background: #1e293b; color: #e2e8f0; padding: 10px 14px; border-radius: 5px;
        overflow-x: auto; font-size: 6.5pt !important; line-height: 1.4; page-break-inside: avoid; }
  pre code { background: none; color: inherit; padding: 0; font-size: 6.5pt !important; }
  pre[class*="language-"] { font-size: 6.5pt !important; }
  code[class*="language-"] { font-size: 6.5pt !important; }
  hr { border: none; border-top: 1px solid #d1d5db; margin: 2em 0; }
  table { border-collapse: collapse; width: 100%; margin: 1em 0; page-break-inside: avoid; }
  th, td { border: 1px solid #d1d5db; padding: 4px 8px; text-align: left; font-size: 8pt; }
  th { background: #f9fafb; font-weight: 600; }
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

  const browser = await puppeteer.launch({ headless: true, args: ["--no-sandbox"] });
  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 794, height: 1123 });  // A4 at 96dpi
    await page.setContent(html, { waitUntil: "networkidle0" });
    await page.pdf({
      path: opts.output,
      format: "A4",
      margin: { top: "0.75in", bottom: "0.75in", left: "0.75in", right: "0.75in" },
      printBackground: true,
      outline: true,
      tagged: true,
      displayHeaderFooter: true,
      headerTemplate: '<span></span>',
      footerTemplate: '<div style="width:100%;text-align:center;font-size:9px;color:#999;">Page <span class="pageNumber"></span> of <span class="totalPages"></span></div>',
    });
    console.log(`PDF generated: ${opts.output}`);
  } finally {
    await browser.close();
  }
}

main();
