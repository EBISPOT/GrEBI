#!/usr/bin/env node
/**
 * GrEBI Documentation PDF Renderer
 *
 * Renders a self-contained HTML file (produced by generate_docs_pdf.mjs
 * --html-out) into a PDF using puppeteer/chromium. This is split out from
 * generate_docs_pdf.mjs so it can run in the upstream-maintained puppeteer
 * image, keeping chromium out of the GrEBI images.
 *
 * Usage:
 *   node render_pdf.mjs --html INPUT.html --output OUTPUT.pdf
 */

import fs from "fs";
import { createRequire } from "module";

// puppeteer lives in the upstream puppeteer image's node_modules, outside this
// script's directory. ESM bare imports don't honour NODE_PATH, so resolve it
// explicitly. Override the base with PUPPETEER_NODE_MODULES if the image layout
// differs.
const puppeteerBase =
  (process.env.PUPPETEER_NODE_MODULES || "/home/pptruser/node_modules") +
  "/puppeteer/package.json";
const require = createRequire(puppeteerBase);

function parseArgs() {
  const args = process.argv.slice(2);
  const opts = { html: null, output: "grebi-docs.pdf" };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--html" && args[i + 1]) opts.html = args[++i];
    else if (args[i] === "--output" && args[i + 1]) opts.output = args[++i];
  }
  return opts;
}

async function main() {
  const opts = parseArgs();

  if (!opts.html || !fs.existsSync(opts.html)) {
    console.error(`ERROR: input HTML not found: ${opts.html}`);
    process.exit(1);
  }

  const finalHtml = fs.readFileSync(opts.html, "utf8");

  console.log(`Generating PDF: ${opts.output}`);
  const puppeteer = require("puppeteer");
  const browser = await puppeteer.launch({ headless: true, args: ["--no-sandbox"] });
  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 794, height: 1123 }); // A4 at 96dpi
    await page.setContent(finalHtml, { waitUntil: "networkidle0" });
    await page.pdf({
      path: opts.output,
      format: "A4",
      margin: { top: "0.75in", bottom: "0.75in", left: "0.75in", right: "0.75in" },
      printBackground: true,
      outline: true,
      tagged: true,
      displayHeaderFooter: true,
      headerTemplate: "<span></span>",
      footerTemplate:
        '<div style="width:100%;text-align:center;font-size:9px;color:#999;">Page <span class="pageNumber"></span> of <span class="totalPages"></span></div>',
    });
    console.log(`PDF generated: ${opts.output}`);
  } finally {
    await browser.close();
  }
}

main();
