import { Fragment, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { get } from "../../../app/api";
import ErrorMessage from "../../../components/ErrorMessage";
import { Inline } from "./OpenApiReference";

/** The arguments every template tool takes beside the template's own parameters. */
export const TEMPLATE_CONTROLS = ["graph", "sortBy", "sortDir", "pageNum", "pageSize"];

export interface McpTool {
  name: string;
  description?: string;
  inputSchema?: { type?: string; properties?: Record<string, any>; required?: string[] };
  outputSchema?: any;
  /** the query template the tool runs, when it is one */
  template?: string;
  /** the graphs the template names; absent when it runs on any graph the instance serves */
  graphs?: string[];
}

export interface McpCatalogue {
  server: { name: string; version: string; endpoint: string; transport: string };
  instructions: string;
  graphs: string[];
  resources: { uri: string; name: string; description?: string; mimeType?: string }[];
  tools: McpTool[];
}

/** A property's type in a word or two: its enum, its type, or a list of types. */
export function propertyType(schema: any): string {
  if (!schema) return "";
  if (schema.enum) return schema.enum.join(" | ");
  if (Array.isArray(schema.type)) return schema.type.join(" | ");
  if (schema.type === "array") return `array of ${propertyType(schema.items) || "any"}`;
  return schema.type || "";
}

/** The template tools a graph the instance serves offers: those naming it, and those that run on any graph. */
export function toolsForGraph(catalogue: McpCatalogue, graph: string): McpTool[] {
  return catalogue.tools.filter((t) => t.template && (!t.graphs || t.graphs.includes(graph)));
}

/**
 * The template tools none of whose graphs this instance serves: known to
 * this version of GrEBI, callable on an instance that serves their graph.
 */
export function otherTemplates(catalogue: McpCatalogue): McpTool[] {
  return catalogue.tools.filter((t) => t.template && t.graphs && !t.graphs.some((g) => catalogue.graphs.includes(g)));
}

/** A template tool's title: what precedes the colon in its description. */
export function titleOf(tool: McpTool): string {
  const description = tool.description || "";
  const colon = description.indexOf(": ");
  return colon > 0 ? description.slice(0, colon) : description;
}

/** The result columns of a template tool, from its output schema's rows. */
export function resultColumns(tool: McpTool): [string, string][] {
  const columns = tool.outputSchema?.properties?.rows?.items?.properties || {};
  return Object.entries(columns).map(([name, schema]) => [name, propertyType(schema)]);
}

function Arguments({ properties, required, only }: { properties: Record<string, any>; required: string[]; only?: (name: string) => boolean }) {
  const names = Object.keys(properties).filter((n) => !only || only(n));
  if (names.length === 0) return <p className="text-sm text-gray-500 my-1">No arguments of its own.</p>;
  return (
    <table className="w-full text-sm my-2 border border-gray-200">
      <thead>
        <tr className="bg-gray-50 text-left text-gray-600">
          <th className="py-1 px-2 font-medium">Argument</th>
          <th className="py-1 px-2 font-medium">Type</th>
          <th className="py-1 px-2 font-medium">Description</th>
        </tr>
      </thead>
      <tbody>
        {names.map((name) => (
          <tr key={name} className="border-t border-gray-100 align-top">
            <td className="py-1 px-2 whitespace-nowrap"><code>{name}</code>{required.includes(name) && <span className="text-red-700" title="required"> *</span>}</td>
            <td className="py-1 px-2 text-gray-600">{propertyType(properties[name])}</td>
            <td className="py-1 px-2">
              {properties[name]?.description && <Inline text={properties[name].description} />}
              {properties[name]?.default !== undefined && <span className="text-gray-500"> Default <code>{String(properties[name].default)}</code>.</span>}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function FixedTool({ tool }: { tool: McpTool }) {
  const outputs = Object.keys(tool.outputSchema?.properties || {});
  return (
    <div className="my-5 border border-gray-200 rounded-lg overflow-hidden">
      <div className="bg-gray-50 px-4 py-2 border-b border-gray-200"><code className="font-mono font-semibold">{tool.name}</code></div>
      <div className="px-4 py-3">
        {tool.description && <p className="text-sm text-gray-700 my-1"><Inline text={tool.description} /></p>}
        <Arguments properties={tool.inputSchema?.properties || {}} required={tool.inputSchema?.required || []} />
        {outputs.length > 0 && <p className="text-sm text-gray-600 my-1">Returns {outputs.map((o, i) => <Fragment key={o}>{i > 0 ? ", " : ""}<code>{o}</code></Fragment>)}.</p>}
      </div>
    </div>
  );
}

function TemplateTool({ tool, graph }: { tool: McpTool; graph: string }) {
  const properties = tool.inputSchema?.properties || {};
  const sortBy: string[] = properties.sortBy?.enum || [];
  const columns = resultColumns(tool);
  return (
    <div className="my-5 border border-gray-200 rounded-lg overflow-hidden">
      <div className="bg-gray-50 px-4 py-2 flex flex-wrap items-center gap-3 border-b border-gray-200">
        <code className="font-mono font-semibold">{tool.name}</code>
        {!tool.graphs && <span className="text-xs text-gray-600">runs on any graph the instance serves</span>}
        <Link className="link-default text-sm ml-auto" to={`/graphs/${graph}/queries/${encodeURIComponent(tool.template || tool.name)}`}>Run it in the browser</Link>
      </div>
      <div className="px-4 py-3">
        {tool.description && <p className="text-sm text-gray-700 my-1"><Inline text={tool.description} /></p>}
        <Arguments properties={properties} required={tool.inputSchema?.required || []} only={(n) => !TEMPLATE_CONTROLS.includes(n)} />
        {sortBy.length > 0 && <p className="text-sm text-gray-600 my-1">Sort by {sortBy.map((c, i) => <Fragment key={c}>{i > 0 ? ", " : ""}<code>{c}</code></Fragment>)}.</p>}
        {columns.length > 0 && (
          <p className="text-sm text-gray-600 my-1">Result columns: {columns.map(([name, type], i) => <Fragment key={name}>{i > 0 ? ", " : ""}<code>{name}</code>{type ? ` (${type})` : ""}</Fragment>)}.</p>
        )}
      </div>
    </div>
  );
}

/**
 * The MCP server's catalogue as a reference: its instructions and resources,
 * the tools that work on every graph, then the template tools under each
 * graph they are for, since every graph has its own query templates.
 */
export default function McpReference({ catalogue: given }: { catalogue?: McpCatalogue }) {
  const [catalogue, setCatalogue] = useState<McpCatalogue | null>(given ?? null);
  const [error, setError] = useState<any>(null);

  useEffect(() => {
    if (given) return;
    let cancelled = false;
    get<McpCatalogue>("api/v1/mcp/catalogue")
      .then((c) => { if (!cancelled) setCatalogue(c); })
      .catch((e) => { if (!cancelled) setError(e); });
    return () => { cancelled = true; };
  }, [given]);

  if (error) return <ErrorMessage what="The MCP catalogue" error={error} />;
  if (!catalogue) return <p className="text-gray-500">Loading the MCP catalogue…</p>;

  const fixed = catalogue.tools.filter((t) => !t.template);
  const others = otherTemplates(catalogue);

  return (
    <div className="mcp-reference">
      <p className="text-sm text-gray-600">Server <code>{catalogue.server.name}</code> {catalogue.server.version}, endpoint <code>{catalogue.server.endpoint}</code> ({catalogue.server.transport}). This instance serves {catalogue.graphs.map((g, i) => <Fragment key={g}>{i > 0 ? ", " : ""}<code>{g}</code></Fragment>)}.</p>
      <h2 id="mcp-instructions" className="text-xl font-bold mt-6 mb-2">Instructions to agents</h2>
      <blockquote className="border-l-4 border-gray-300 pl-3 text-gray-700 my-2">{catalogue.instructions.split(/\n\s*\n/).map((p, i) => <p key={i}>{p.replace(/\s*\n\s*/g, " ")}</p>)}</blockquote>

      <h2 id="mcp-resources" className="text-xl font-bold mt-6 mb-2">Resources</h2>
      <table className="w-full text-sm my-2 border border-gray-200">
        <thead>
          <tr className="bg-gray-50 text-left text-gray-600">
            <th className="py-1 px-2 font-medium">URI</th>
            <th className="py-1 px-2 font-medium">Name</th>
            <th className="py-1 px-2 font-medium">Type</th>
          </tr>
        </thead>
        <tbody>
          {catalogue.resources.map((r) => (
            <tr key={r.uri} className="border-t border-gray-100">
              <td className="py-1 px-2"><code>{r.uri}</code></td>
              <td className="py-1 px-2">{r.name}{r.description ? <span className="text-gray-600">: {r.description}</span> : null}</td>
              <td className="py-1 px-2 text-gray-600">{r.mimeType}</td>
            </tr>
          ))}
        </tbody>
      </table>

      <h2 id="mcp-tools" className="text-xl font-bold mt-6 mb-2">Tools for every graph</h2>
      {fixed.map((t) => <FixedTool key={t.name} tool={t} />)}

      {catalogue.graphs.map((graph) => {
        const tools = toolsForGraph(catalogue, graph);
        return (
          <section key={graph}>
            <h2 id={`mcp-tools-${graph}`} className="text-xl font-bold mt-8 mb-2">Tools for <code>{graph}</code></h2>
            {tools.length === 0
              ? <p className="text-sm text-gray-600">No query templates for this graph.</p>
              : tools.map((t) => <TemplateTool key={t.name} tool={t} graph={graph} />)}
          </section>
        );
      })}
      {others.length > 0 && (
        <section>
          <h2 id="mcp-other-templates" className="text-xl font-bold mt-8 mb-2">Templates for other graphs</h2>
          <p className="text-sm text-gray-600">Query templates in this version of GrEBI for graphs this instance does not serve. An instance serving one of these graphs offers them as tools, with the same arguments as above.</p>
          <table className="w-full text-sm my-2 border border-gray-200">
            <thead>
              <tr className="bg-gray-50 text-left text-gray-600">
                <th className="py-1 px-2 font-medium">Template</th>
                <th className="py-1 px-2 font-medium">Question</th>
                <th className="py-1 px-2 font-medium">Graphs</th>
              </tr>
            </thead>
            <tbody>
              {others.map((t) => (
                <tr key={t.name} className="border-t border-gray-100 align-top">
                  <td className="py-1 px-2"><code>{t.name}</code></td>
                  <td className="py-1 px-2">{titleOf(t)}</td>
                  <td className="py-1 px-2">{(t.graphs || []).map((g, i) => <Fragment key={g}>{i > 0 ? ", " : ""}<code>{g}</code></Fragment>)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}
    </div>
  );
}
