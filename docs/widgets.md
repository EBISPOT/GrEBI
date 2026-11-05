
# Embedding widgets

GrEBI's pages can be embedded in your own website as widgets: a node and everything around it, a query and its results, a search box over a graph. Each widget is a custom HTML element, `<grebi-node>`, `<grebi-query>` and so on, that renders the same interface as GrEBI itself inside your page and talks to the GrEBI API from there. Links inside a widget stay inside it, so a visitor can follow edges from node to node, or go from a query's results to a node, without leaving your page; every widget has an **Open in GrEBI** link to the same view on the GrEBI site, and a **Back** button once it has navigated.

## Setup

Include the widgets script once, anywhere on the page:

```html
<script src="https://www.ebi.ac.uk/spot/kg/widgets.js"></script>
```

Then put the elements wherever the widgets should appear:

```html
<grebi-node graph="dismech" node="hgnc:1100"></grebi-node>
```

A widget fills the width of the element that holds it and takes the height its content needs, like any block element, so it can be sized and placed with CSS on the element itself. Its own styles live in a shadow root and do not reach your page, nor your page's stylesheet it, apart from the inherited font size. The script loads the fonts GrEBI uses into your page.

The widgets talk to the GrEBI instance the script came from. To point them at another, for example a local deployment, name its API on the script tag:

```html
<script src="http://localhost:3000/widgets.js" data-api="http://localhost:3000/"></script>
```

## The elements

Every element names a graph, as in GrEBI's URLs (`dismech`, `impc`, …) and the page it shows with a few attributes of its own, listed below. Any other attribute, apart from the usual HTML ones (`class`, `style`, `id`, `title`, …), is passed to the page as its query string, so the same options as in the page's URL on the GrEBI site are available: a node's `tab`, a query's parameters, a search's `q`. Change an attribute and the widget shows the new page.

### `<grebi-node>`

A node: its names, identifiers and description, with its properties, its edges in and out, links to other resources and similar nodes in tabs, as on the node's page in GrEBI, and a search box for another node. The visitor can follow the edges to other nodes inside the widget. The graph view is a widget of its own, `<grebi-graph>` below.

```html
<grebi-node graph="dismech" node="hgnc:1100"></grebi-node>
```

| Attribute | |
|-----------|---|
| `graph` | The graph. |
| `node` | The node, by its CURIE (`hgnc:1100`, `MONDO:0005083`) or any other identifier it carries, as the API's node lookup resolves it. |
| `tab` | The tab to open: `properties` (the default), `edges_in`, `edges_out`, `links` or `similar`. |
| `lang` | The language of the node's labels, where it has several. |

<grebi-node graph="dismech" node="hgnc:1100"></grebi-node>

### `<grebi-graph>`

The graph view of a node: the node and its neighbours drawn as a graph, with the controls to expand edges by type and datasource, lay the graph out and follow it to other nodes, as on the Graph tab of the node's page in GrEBI. It draws with WebGL; in a browser without it the widget says the graph view is not available.

```html
<grebi-graph graph="dismech" node="hgnc:1100"></grebi-graph>
```

| Attribute | |
|-----------|---|
| `graph` | The graph. |
| `node` | The node to start from, as for `<grebi-node>`. |
| `g` | An exploration to start with: the `g` parameter of a graph view's URL on the GrEBI site, which records the edges expanded. |

<grebi-graph graph="dismech" node="hgnc:1100"></grebi-graph>

### `<grebi-query>`

A query template with its parameters. Given every parameter, the widget shows the results alone: the table, with its filters, sorting, paging and CSV download, and the query's title and question above it. Leave a parameter out, or add `inputs="true"`, and the widget shows the inputs form and the template's examples too, so that visitors can ask their own question.

```html
<grebi-query graph="dismech" query="disease_to_treatments" disease_id="mondo:0005002"></grebi-query>
```

| Attribute | |
|-----------|---|
| `graph` | The graph. |
| `query` | The query template's id, as in its URL on GrEBI and in the API. |
| `inputs` | `true` to show the inputs form even when every parameter is given. |
| *the parameters* | One attribute per parameter of the template, with the names and values the API takes (a `SourceId` parameter takes a CURIE). |

<grebi-query graph="dismech" query="disease_to_treatments" disease_id="mondo:0005002"></grebi-query>

### `<grebi-queries>`

The list of a graph's query templates, with their topics to narrow it, from which the visitor picks one to run.

```html
<grebi-queries graph="dismech"></grebi-queries>
<grebi-queries graph="dismech" topic="gwas"></grebi-queries>
```

| Attribute | |
|-----------|---|
| `graph` | The graph. |
| `topic` | A topic to start with ticked, by its id or a unique prefix of it. |

### `<grebi-search>`

Search over a graph's nodes: the search box with its type and datasource filters, and the results, each leading to its node inside the widget.

```html
<grebi-search graph="dismech"></grebi-search>
<grebi-search graph="dismech" q="BRCA1"></grebi-search>
```

| Attribute | |
|-----------|---|
| `graph` | The graph. |
| `q` | A search to start with. |

### `<grebi-lookup>`

The identifier lookup: a list of CURIEs, IRIs or accessions, resolved to their nodes in one go, with a CSV download of the answer.

```html
<grebi-lookup graph="dismech"></grebi-lookup>
```

| Attribute | |
|-----------|---|
| `graph` | The graph. |
| `ids` | Identifiers to start with, separated by whitespace or commas. |

### `<grebi-edge>`

One edge, by its id: its type, its two nodes and its properties, as on the edge's page.

```html
<grebi-edge graph="dismech" edge="..."></grebi-edge>
```

| Attribute | |
|-----------|---|
| `graph` | The graph. |
| `edge` | The edge's id, as the API returns it. |

### `<grebi-page>`

Any other page of GrEBI, by its path on the site: the graph's own page, its tables, its downloads.

```html
<grebi-page path="/graphs/dismech/tables"></grebi-page>
```

| Attribute | |
|-----------|---|
| `path` | The page's path, starting with `/`, with a query string if it needs one. |

## Behaviour

**Navigation.** A widget has a history of its own, separate from your page's: links inside it change what the widget shows, the **Back** button in its bar goes back through them, and your page's address and history are untouched. A middle click or a modified click on a link, which asks for a new tab, opens the page on the GrEBI site, as does **Open in GrEBI**. Links out of GrEBI, to the resources a node comes from or to publications, open in a new tab as they do on the site.

**Several widgets.** A page can hold any number of widgets, of any kinds; each is independent. The script is loaded once, whichever way they are added, and elements added to the page later, by a script of your own, become widgets as well.

**Styling.** The widget's interface is GrEBI's; it is not themed. It is a white block, the width of its container, with the text size your page gives it. Put a border, margins or a maximum height (with `overflow: auto`) on the element itself as you would on any block.

**Without JavaScript.** The elements render nothing on their own. Where a script cannot be included, link to the page on the GrEBI site instead.
