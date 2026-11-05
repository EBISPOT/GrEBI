import { createRoot, Root } from "react-dom/client";
import urlJoin from "url-join";
import encodeNodeId from "../encodeNodeId";
import { siteUrl } from "../app/instance";
import GrebiWidget from "./GrebiWidget";

/**
 * The widget elements, <grebi-node>, <grebi-query> and the rest: one per kind
 * of page, documented on the "Embedding widgets" page of the docs. A few
 * attributes name the page; every other attribute, apart from the HTML ones,
 * becomes the page's query string, so a node's `tab` or a query's parameters
 * are attributes too. The element renders the page (GrebiWidget) in a shadow
 * root of its own, with the site's stylesheet.
 */

type Kind = {
  // the attributes that name the page
  attrs: string[];
  // the page's path from them, or null while one is missing
  path(a: Record<string, string>): string | null;
};

const enc = encodeURIComponent;

export const KINDS: Record<string, Kind> = {
  "grebi-node": { attrs: ["graph", "node"], path: (a) => (a.graph && a.node ? `/graphs/${enc(a.graph)}/nodes/${encodeNodeId(a.node)}` : null) },
  // the node page's graph view alone (EbiNodePage shows it so inside a widget)
  "grebi-graph": { attrs: ["graph", "node"], path: (a) => (a.graph && a.node ? `/graphs/${enc(a.graph)}/nodes/${encodeNodeId(a.node)}?tab=graph` : null) },
  "grebi-edge": { attrs: ["graph", "edge"], path: (a) => (a.graph && a.edge ? `/graphs/${enc(a.graph)}/edges/${encodeNodeId(a.edge)}` : null) },
  "grebi-query": { attrs: ["graph", "query"], path: (a) => (a.graph && a.query ? `/graphs/${enc(a.graph)}/queries/${enc(a.query)}` : null) },
  "grebi-queries": { attrs: ["graph", "topic"], path: (a) => (a.graph ? `/graphs/${enc(a.graph)}/queries` + (a.topic ? `/${enc(a.topic)}` : "") : null) },
  "grebi-search": { attrs: ["graph"], path: (a) => (a.graph ? `/graphs/${enc(a.graph)}/search` : null) },
  "grebi-lookup": { attrs: ["graph"], path: (a) => (a.graph ? `/graphs/${enc(a.graph)}/lookup` : null) },
  "grebi-page": { attrs: ["path"], path: (a) => (a.path && a.path.startsWith("/") ? a.path : null) },
};

// the attributes of any HTML element, which are the element's, not the page's
const HTML_ATTRIBUTES = new Set(["class", "style", "id", "title", "hidden", "slot", "dir", "tabindex", "role", "part"]);

/** The page a widget element shows, as a path with a query string, or null while it names none. */
export function widgetPath(el: Element): string | null {
  const kind = KINDS[el.localName];
  if (!kind) {
    return null;
  }
  const own: Record<string, string> = {};
  const rest = new URLSearchParams();
  for (const attr of Array.from(el.attributes)) {
    const name = attr.name.toLowerCase();
    if (kind.attrs.includes(name)) {
      own[name] = attr.value;
    } else if (!HTML_ATTRIBUTES.has(name) && !name.startsWith("data-") && !name.startsWith("aria-")) {
      rest.append(attr.name, attr.value);
    }
  }
  const path = kind.path(own);
  if (!path) {
    return null;
  }
  const qs = rest.toString();
  return qs ? path + (path.includes("?") ? "&" : "?") + qs : path;
}

class GrebiElement extends HTMLElement {
  private root: Root | null = null;
  private container: HTMLElement | null = null;
  private changes: MutationObserver | null = null;

  connectedCallback() {
    if (!this.shadowRoot) {
      const shadow = this.attachShadow({ mode: "open" });
      const sheet = document.createElement("link");
      sheet.rel = "stylesheet";
      sheet.href = urlJoin(siteUrl(), "/widgets.css");
      shadow.appendChild(sheet);
      this.container = document.createElement("div");
      shadow.appendChild(this.container);
    }
    if (!this.root) {
      this.root = createRoot(this.container!);
    }
    this.render();
    this.changes = new MutationObserver(() => this.render());
    this.changes.observe(this, { attributes: true });
  }

  disconnectedCallback() {
    this.changes?.disconnect();
    this.changes = null;
    this.root?.unmount();
    this.root = null;
  }

  private render() {
    const path = widgetPath(this);
    this.root!.render(path ? <GrebiWidget path={path} shadowRoot={this.shadowRoot!} container={this.container!} /> : null);
  }
}

/** Defines every widget element, once. */
export function defineGrebiWidgets() {
  for (const tag of Object.keys(KINDS)) {
    if (!customElements.get(tag)) {
      customElements.define(tag, class extends GrebiElement {});
    }
  }
}

const FONTS = [
  "https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:ital,wght@0,100;0,200;0,300;0,400;0,500;0,600;0,700;1,100;1,200;1,300;1,400;1,500;1,600;1,700&display=swap",
  "https://ebi.emblstatic.net/web_guidelines/EBI-Icon-fonts/v1.3/fonts.css",
];

/**
 * The site's fonts, into the host document's head: a shadow root can use a
 * font only when the document declares it. Once per document.
 */
export function loadFonts(doc: Document) {
  if (doc.querySelector("link[data-grebi-fonts]")) {
    return;
  }
  for (const href of FONTS) {
    const link = doc.createElement("link");
    link.rel = "stylesheet";
    link.href = href;
    link.setAttribute("data-grebi-fonts", "");
    doc.head.appendChild(link);
  }
}
