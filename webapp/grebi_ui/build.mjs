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
let docsManifest = { sidebar: [], pages: {}, images: {} };
if (fs.existsSync(docsDir)) {
  const sidebarPath = path.join(docsDir, "_sidebar.yaml");
  if (fs.existsSync(sidebarPath)) {
    docsManifest.sidebar = yaml.load(fs.readFileSync(sidebarPath, "utf8"));
  }
  for (const file of fs.readdirSync(docsDir)) {
    if (file.endsWith(".md")) {
      const slug = file.replace(/\.md$/, "");
      docsManifest.pages[slug] = fs.readFileSync(path.join(docsDir, file), "utf8");
    }
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
  console.log(`  Found ${Object.keys(docsManifest.pages).length} doc pages, ${Object.keys(docsManifest.images).length} images`);
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


