import { FormEvent, Fragment, useEffect, useRef, useState } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";
import EbiBreadcrumbsBar from "../EbiBreadcrumbsBar";
import { post } from "../../../app/api";
import GraphNodeRef from "../../../model/GraphNodeRef";
import PropVal from "../../../model/PropVal";
import { DatasourceTags } from "../../../components/DatasourceTag";
import ErrorMessage from "../../../components/ErrorMessage";
import LoadingOverlay from "../../../components/LoadingOverlay";

type LookupResult = { id: string; nodes: any[] };
export type LookupAnswer = { results: LookupResult[]; notFound: string[]; truncated: boolean };

/** The identifiers in what was pasted: one per line, or separated by commas, semicolons or tabs. Repeats are dropped. */
export function splitIdentifiers(text: string): string[] {
  const ids = new Set<string>();
  for (const part of text.split(/[\r\n,;\t]+/)) {
    const id = part.trim();
    if (id) ids.add(id);
  }
  return [...ids];
}

/** The answer as CSV: a row per identifier and node; an identifier without a node has empty node columns. */
export function lookupCsv(answer: LookupAnswer): string {
  const cell = (value: any): string => {
    const text = value === undefined || value === null ? "" : String(value);
    return /[",\r\n]/.test(text) ? '"' + text.replace(/"/g, '""') + '"' : text;
  };
  const lines = [["identifier", "nodeId", "name", "type", "datasources", "curie"].join(",")];
  for (const result of answer.results) {
    if (result.nodes.length === 0) {
      lines.push([cell(result.id), "", "", "", "", ""].join(","));
      continue;
    }
    for (const node of result.nodes) {
      const ref = new GraphNodeRef(node);
      lines.push([
        cell(result.id),
        cell(ref.getNodeId()),
        cell(ref.getName()),
        cell(PropVal.arrFrom(node["grebi:type"]).map((t) => t.value).join("|")),
        cell(ref.getDatasources().join("|")),
        cell(ref.getId().value),
      ].join(","));
    }
  }
  return lines.join("\n") + "\n";
}

/** Longer lists than this stay out of the URL, which otherwise records the lookup so it can be linked to. */
const MAX_IDS_IN_URL = 4000;

/**
 * Many identifiers at once: which node each one names, and which name nothing.
 * A URL with ?ids=a,b runs the lookup on arrival; matchNames=true is honoured too.
 */
export default function EbiLookupPage() {
  const params = useParams();
  const graph = params.graph as string;
  const [searchParams, setSearchParams] = useSearchParams();

  const [text, setText] = useState(() => splitIdentifiers(searchParams.get("ids") || "").join("\n"));
  const [matchNames, setMatchNames] = useState(searchParams.get("matchNames") === "true");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<any>(null);
  const [answer, setAnswer] = useState<LookupAnswer | null>(null);

  const ids = splitIdentifiers(text);

  async function lookUp(ids: string[], matchNames: boolean) {
    if (ids.length === 0) return;
    setLoading(true);
    setError(null);
    try {
      const res = await post<{ ids: string[] }, LookupAnswer>(
        `api/v1/graphs/${graph}/lookup`,
        matchNames ? { matchNames: "true" } : undefined,
        { ids }
      );
      setAnswer(res);
    } catch (e) {
      setError(e);
    } finally {
      setLoading(false);
    }
  }

  function submit(e: FormEvent) {
    e.preventDefault();
    const joined = ids.join(",");
    const next = new URLSearchParams(searchParams);
    if (joined.length <= MAX_IDS_IN_URL) next.set("ids", joined); else next.delete("ids");
    if (matchNames) next.set("matchNames", "true"); else next.delete("matchNames");
    setSearchParams(next, { replace: true });
    lookUp(ids, matchNames);
  }

  // a lookup arrived at by its URL runs once, on arrival
  const arrived = useRef(false);
  useEffect(() => {
    if (arrived.current) return;
    arrived.current = true;
    const fromUrl = splitIdentifiers(searchParams.get("ids") || "");
    if (fromUrl.length > 0) lookUp(fromUrl, searchParams.get("matchNames") === "true");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function download() {
    if (!answer) return;
    const url = URL.createObjectURL(new Blob([lookupCsv(answer)], { type: "text/csv" }));
    const a = document.createElement("a");
    a.href = url;
    a.download = `${graph}_lookup.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  const found = answer ? answer.results.filter((r) => r.nodes.length > 0).length : 0;

  return (
    <Fragment>
      <EbiBreadcrumbsBar graph={graph} entries={[
        { url: `/graphs`, label: "Graphs" },
        { url: `/graphs/${graph}/lookup`, label: "Identifier lookup" }
      ]} />
      <main className="container mx-auto px-4 my-8">
        <div className="text-2xl font-bold my-6">Look up identifiers</div>
        <p className="px-1 mb-4">
          Paste CURIEs such as <code>mondo:0005083</code> or <code>HGNC:1100</code>, IRIs or database accessions, one per line.
          Each is matched against every identifier a node has, whatever the case of its prefix.
          To find nodes by text instead, <Link className="link-default" to={`/graphs/${graph}/search`}>search</Link>.
        </p>
        <form onSubmit={submit}>
          <textarea
            aria-label="Identifiers"
            className="w-full h-40 p-2 font-mono text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-link-default"
            placeholder={"mondo:0005083\nHGNC:1100\nhttp://purl.obolibrary.org/obo/CHEBI_15365"}
            value={text}
            onChange={(e) => setText(e.target.value)}
          />
          <div className="flex flex-wrap items-center gap-4 mt-2">
            <button
              type="submit"
              className="px-4 py-2 rounded-md bg-link-default text-white font-medium hover:bg-link-dark disabled:opacity-50 disabled:cursor-not-allowed"
              disabled={ids.length === 0 || loading}
            >
              {ids.length > 0 ? `Look up ${ids.length} identifier${ids.length === 1 ? "" : "s"}` : "Look up identifiers"}
            </button>
            <label htmlFor="match-names" className="block p-1 w-fit">
              <input
                type="checkbox"
                id="match-names"
                className="invisible hidden peer"
                checked={matchNames}
                onChange={(e) => setMatchNames(e.target.checked)}
              />
              <span className="input-checkbox mr-4" />
              <span className="mr-4">Also match names (slower)</span>
            </label>
          </div>
        </form>
        {error && <div className="mt-4"><ErrorMessage what="The identifiers" error={error} /></div>}
        {loading && <LoadingOverlay message="Looking up identifiers..." />}
        {answer && (
          <div className="mt-6">
            <div className="flex flex-wrap items-center justify-between gap-4 mb-2">
              <div className="font-semibold">
                {found} of {answer.results.length} identifier{answer.results.length === 1 ? "" : "s"} found
                {answer.truncated && <span className="font-normal text-gray-600"> (some nodes were left out: too many matched)</span>}
              </div>
              <button className="link-default text-sm" onClick={download}>Download as CSV</button>
            </div>
            <table className="w-full text-sm border border-gray-200 rounded-lg overflow-hidden">
              <thead>
                <tr className="bg-gray-50 text-left text-gray-600 border-b border-gray-200">
                  <th className="py-2 px-3 font-medium">Identifier</th>
                  <th className="py-2 px-3 font-medium">Node</th>
                  <th className="py-2 px-3 font-medium">Type</th>
                  <th className="py-2 px-3 font-medium">Datasources</th>
                </tr>
              </thead>
              <tbody>
                {answer.results.flatMap((result, i) => {
                  const shade = i % 2 === 1 ? "bg-gray-50" : "";
                  if (result.nodes.length === 0) {
                    return [
                      <tr key={result.id} className={`border-b border-gray-100 ${shade}`}>
                        <td className="py-2 px-3 font-mono">{result.id}</td>
                        <td className="py-2 px-3 text-gray-500 italic" colSpan={3}>No node has this identifier</td>
                      </tr>
                    ];
                  }
                  return result.nodes.map((node) => {
                    const ref = new GraphNodeRef(node);
                    const type = ref.extractType();
                    // a type the UI has no name for is shown as it is
                    const typeName = type ? type.longName : (PropVal.arrFrom(node["grebi:type"])[0]?.value ?? "");
                    return (
                      <tr key={`${result.id}\t${ref.getNodeId()}`} className={`border-b border-gray-100 ${shade}`}>
                        <td className="py-2 px-3 font-mono">{result.id}</td>
                        <td className="py-2 px-3">
                          <Link className="link-default" to={`/graphs/${graph}/nodes/${ref.getEncodedNodeId()}`}>{ref.getName()}</Link>
                        </td>
                        <td className="py-2 px-3 text-gray-700">{typeName}</td>
                        <td className="py-2 px-3"><DatasourceTags dss={ref.getDatasources()} linked /></td>
                      </tr>
                    );
                  });
                })}
              </tbody>
            </table>
          </div>
        )}
      </main>
    </Fragment>
  );
}
