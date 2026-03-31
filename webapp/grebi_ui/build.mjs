import { exec } from "child_process";
import { build } from "esbuild";
import fs from "fs";
import path from "path";
import tw from 'tailwindcss';
import postcss from 'postcss';
import atImport from 'postcss-import';
import yaml from 'js-yaml';

let define = {};
for (const k in process.env) {
  define[`process.env.${k}`] = JSON.stringify(process.env[k]);
}

///
/// Build docs manifest (reads docs/ and bundles into a JSON file)
///
console.log("### Building docs manifest");
// Always read from the repo root docs/ directory (the single source of truth)
let docsDir = path.resolve("docs");
const rootDocsDir = path.resolve("../../docs");
if (fs.existsSync(rootDocsDir)) {
  docsDir = rootDocsDir;
}
// Build sidebar tree from markdown headings (h1–h4).
// Each heading becomes an anchor slug; nesting follows heading depth.
function sidebarFromHeadings(md) {
  const headingRe = /^(#{1,4})\s+(.+)$/gm;
  const flat = [];
  let m;
  while ((m = headingRe.exec(md)) !== null) {
    const level = m[1].length;               // 1–4
    const title = m[2].replace(/\\(.)/g, '$1').trim();
    const anchor = title.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
    flat.push({ level, title, anchor });
  }
  // Build nested tree: children go under the nearest preceding lower-level heading
  const root = [];
  const stack = [{ children: root, level: 0 }];
  for (const h of flat) {
    const entry = { title: h.title, anchor: h.anchor };
    while (stack.length > 1 && stack[stack.length - 1].level >= h.level) stack.pop();
    stack[stack.length - 1].children.push(entry);
    stack.push({ children: (entry.children = []), level: h.level });
  }
  // Strip empty children arrays
  function prune(entries) {
    for (const e of entries) {
      if (e.children && e.children.length) prune(e.children);
      else delete e.children;
    }
  }
  prune(root);
  return root;
}
// Resolve <include src="path/to/file.md" /> tags recursively.
// Paths are relative to the directory of the file containing the tag.
function resolveIncludes(md, baseDir, seen = new Set()) {
  return md.replace(/^<include\s+src=["']([^"']+)["']\s*\/>$/gm, (_match, relPath) => {
    const absPath = path.resolve(baseDir, relPath);
    if (seen.has(absPath)) {
      console.warn(`  Warning: circular include detected for ${relPath}, skipping`);
      return '';
    }
    if (!fs.existsSync(absPath)) {
      console.warn(`  Warning: included file not found: ${relPath}`);
      return '';
    }
    seen.add(absPath);
    const content = fs.readFileSync(absPath, 'utf8');
    return resolveIncludes(content, path.dirname(absPath), seen);
  });
}
let docsManifest = { sidebar: [], pages: [], images: {} };
if (fs.existsSync(docsDir)) {
  const indexPath = path.join(docsDir, "index.md");
  if (fs.existsSync(indexPath)) {
    const raw = fs.readFileSync(indexPath, "utf8");
    const fullContent = resolveIncludes(raw, docsDir);
    docsManifest.sidebar = sidebarFromHeadings(fullContent);
    // Split content into pages by h1 headings
    const h1Re = /^(?=# [^#])/gm;
    const parts = fullContent.split(h1Re).filter(p => p.trim());
    docsManifest.pages = parts.map(part => {
      const titleMatch = part.match(/^# (.+)$/m);
      const title = titleMatch ? titleMatch[1].trim() : "Untitled";
      const anchor = title.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
      return { title, anchor, content: part };
    });
  }
  const imagesDir = path.join(docsDir, "images");
  if (fs.existsSync(imagesDir)) {
    for (const img of fs.readdirSync(imagesDir)) {
      const ext = path.extname(img).toLowerCase();
      const mimeTypes = { '.svg': 'image/svg+xml', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.gif': 'image/gif', '.webp': 'image/webp' };
      const mime = mimeTypes[ext];
      if (mime) {
        const data = fs.readFileSync(path.join(imagesDir, img));
        docsManifest.images[img] = `data:${mime};base64,${data.toString("base64")}`;
      }
    }
  }
  console.log(`  Found ${docsManifest.pages.length} pages, ${Object.keys(docsManifest.images).length} images, ${docsManifest.sidebar.length} top-level sections`);
} else {
  console.log("  No docs/ directory found — docs manifest will be empty");
}
fs.writeFileSync("dist/docs-manifest.json", JSON.stringify(docsManifest));

///
/// Build index.html (simple find and replace)
///
console.log("### Building index.html");
var public_url = process.env.PUBLIC_URL.endsWith("/") ? process.env.PUBLIC_URL : process.env.PUBLIC_URL + "/";
fs.writeFileSync(
  "dist/index.html",
  fs
    .readFileSync("index.html.in")
    .toString()
    .split("%PUBLIC_URL%/")
    .join(public_url || "/")
    .split("%PUBLIC_URL%")
    .join(public_url || "/")
);

///
/// Build bundle.js (esbuild)
///
console.log("### Building bundle.js");
build({
  entryPoints: [`src/index_${process.env.GREBI_FRONTEND}.tsx`],
  bundle: true,
  platform: "browser",
  outfile: "dist/bundle.js",
  define,
  plugins: [],
  logLevel: "info",
  sourcemap: "linked",

  ...(process.env.GREBI_MINIFY === "true"
    ? {
        minify: true,
      }
    : {}),
});

///
/// Build styles.css (tailwind)
///
console.log("### Building styles.css");

postcss()
  .use(atImport({
    path: ['./src/css']
  }))
  .use(tw())
  .process(fs.readFileSync(`./src/css/${process.env.GREBI_FRONTEND}.css`), {
    from: `./src/css/${process.env.GREBI_FRONTEND}.css`
  })
  .then((result) => {
    fs.writeFileSync(`./dist/styles.css`, result.css);
  });

///
/// Copy files
///
// console.log("### Copying misc files");
// exec("cp ./src/banner.txt ./dist"); // home page banner text


