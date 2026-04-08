#!/usr/bin/env node
/**
 * Scans docs markdown files for <pubmed id="..."/> tags and fetches
 * metadata from PubMed E-utilities. Writes docs/pubmed_cache.json.
 *
 * Usage:
 *   node fetch_pubmed_cache.mjs [--docs-dir DIR]
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function parseArgs() {
  const args = process.argv.slice(2);
  const opts = { docsDir: path.join(__dirname, "..", "docs") };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--docs-dir" && args[i + 1]) opts.docsDir = args[++i];
  }
  return opts;
}

function scanForPmids(docsDir) {
  const pmids = new Set();
  const files = fs.readdirSync(docsDir).filter((f) => f.endsWith(".md"));
  for (const file of files) {
    const content = fs.readFileSync(path.join(docsDir, file), "utf8");
    const regex = /<pubmed\s+id="(\d+)"\s*\/>/g;
    let m;
    while ((m = regex.exec(content)) !== null) {
      pmids.add(m[1]);
    }
  }
  return [...pmids];
}

function formatAuthors(authors) {
  if (!authors || authors.length === 0) return "";
  if (authors.length <= 6) {
    return authors.map((a) => a.name).join(", ");
  }
  return authors.slice(0, 6).map((a) => a.name).join(", ") + ", et al";
}

function extractDoi(articleIds) {
  if (!articleIds) return "";
  const entry = articleIds.find((a) => a.idtype === "doi");
  return entry ? entry.value : "";
}

async function fetchPubmedBatch(pmids) {
  const url = `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=${pmids.join(",")}&retmode=json`;
  const r = await fetch(url, { signal: AbortSignal.timeout(30000) });
  if (!r.ok) throw new Error(`PubMed API returned ${r.status}`);
  const data = await r.json();
  const result = {};
  for (const pmid of pmids) {
    const rec = data.result?.[pmid];
    if (!rec || rec.error) {
      console.warn(`  Warning: PMID ${pmid} not found`);
      continue;
    }
    result[pmid] = {
      title: rec.title || "",
      authors: formatAuthors(rec.authors),
      journal: rec.source || "",
      volume: rec.volume || "",
      issue: rec.issue || "",
      pages: rec.pages || "",
      year: (rec.pubdate || "").replace(/\s.*/, ""),
      doi: extractDoi(rec.articleids),
    };
  }
  return result;
}

async function main() {
  const opts = parseArgs();
  const cachePath = path.join(opts.docsDir, "pubmed_cache.json");

  // Load existing cache
  let cache = {};
  if (fs.existsSync(cachePath)) {
    cache = JSON.parse(fs.readFileSync(cachePath, "utf8"));
  }

  const pmids = scanForPmids(opts.docsDir);
  if (pmids.length === 0) {
    console.log("No <pubmed> tags found in docs.");
    return;
  }
  console.log(`Found PMIDs: ${pmids.join(", ")}`);

  // Only fetch ones not already cached
  const toFetch = pmids.filter((p) => !cache[p]);
  if (toFetch.length > 0) {
    console.log(`Fetching ${toFetch.length} new PMID(s) from PubMed...`);
    // Batch in groups of 200 (E-utilities limit)
    for (let i = 0; i < toFetch.length; i += 200) {
      const batch = toFetch.slice(i, i + 200);
      const results = await fetchPubmedBatch(batch);
      Object.assign(cache, results);
    }
  } else {
    console.log("All PMIDs already cached.");
  }

  // Remove PMIDs no longer referenced
  const referenced = new Set(pmids);
  for (const key of Object.keys(cache)) {
    if (!referenced.has(key)) {
      delete cache[key];
    }
  }

  fs.writeFileSync(cachePath, JSON.stringify(cache, null, 2) + "\n");
  console.log(`Cache written to ${cachePath}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
