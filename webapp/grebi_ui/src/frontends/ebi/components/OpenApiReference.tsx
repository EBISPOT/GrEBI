import { Fragment, useEffect, useState } from "react";
import { get } from "../../../app/api";
import ApiExample from "./ApiExample";
import ErrorMessage from "../../../components/ErrorMessage";

const METHODS = ["get", "post", "put", "delete", "patch"];

/** Text with backtick spans as code and double-asterisk spans in bold. */
export function Inline({ text }: { text: string }) {
  const parts = text.split(/(`[^`]*`|\*\*[^*]+\*\*)/);
  return (
    <Fragment>
      {parts.map((part, i) => {
        if (part.startsWith("`") && part.endsWith("`") && part.length > 1) return <code key={i}>{part.slice(1, -1)}</code>;
        if (part.startsWith("**") && part.endsWith("**") && part.length > 4) return <strong key={i}>{part.slice(2, -2)}</strong>;
        return <Fragment key={i}>{part}</Fragment>;
      })}
    </Fragment>
  );
}

/** A description's paragraphs, separated by blank lines. */
function Paragraphs({ text, className }: { text?: string; className?: string }) {
  if (!text) return null;
  return (
    <Fragment>
      {text.trim().split(/\n\s*\n/).map((p, i) => (
        <p key={i} className={className}><Inline text={p.replace(/\s*\n\s*/g, " ")} /></p>
      ))}
    </Fragment>
  );
}

/** A reference into components, or the object itself. */
export function resolve(spec: any, object: any): any {
  if (object && object.$ref) {
    const path = String(object.$ref).replace(/^#\//, "").split("/");
    let found = spec;
    for (const step of path) found = found?.[step];
    return found ?? object;
  }
  return object;
}

/** The parameters of an operation, path-level ones first, references resolved. */
export function operationParameters(spec: any, pathItem: any, op: any): any[] {
  return [...(pathItem.parameters || []), ...(op.parameters || [])].map((p) => resolve(spec, p));
}

/** A schema's type in a word or two. */
export function schemaLabel(spec: any, schema: any): string {
  if (!schema) return "";
  if (schema.$ref) return String(schema.$ref).split("/").pop() || "";
  if (schema.type === "array") return `array of ${schemaLabel(spec, schema.items) || "any"}`;
  if (schema.enum) return schema.enum.join(" | ");
  return schema.type || "";
}

/**
 * The URL a "Try it" block can call for a GET operation: the path with every
 * path parameter's example, and the query parameters that have one. Nothing
 * when a path parameter has no example.
 */
export function tryIt(path: string, parameters: any[]): { url: string; query: Record<string, string> } | null {
  let url = path;
  const query: Record<string, string> = {};
  for (const p of parameters) {
    if (p.in === "path") {
      if (p.example === undefined) return null;
      url = url.replace(`{${p.name}}`, encodeURIComponent(String(p.example)));
    } else if (p.in === "query" && p.example !== undefined && !Array.isArray(p.example)) {
      query[p.name] = String(p.example);
    }
  }
  return { url, query };
}

function slug(text: string): string {
  return text.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

function Operation({ spec, method, path, pathItem, op }: { spec: any; method: string; path: string; pathItem: any; op: any }) {
  const parameters = operationParameters(spec, pathItem, op);
  const responses = Object.entries(op.responses || {}).map(([status, r]) => [status, resolve(spec, r)] as [string, any]);
  const body = op.requestBody ? resolve(spec, op.requestBody) : null;
  const bodyType = body ? Object.keys(body.content || {})[0] : null;
  const bodyExample = body && bodyType ? body.content[bodyType].example : undefined;
  const example = method === "get" ? tryIt(path, parameters) : null;
  const methodColor = method === "get" ? "bg-blue-600" : method === "post" ? "bg-green-700" : "bg-gray-700";

  return (
    <div className="my-5 border border-gray-200 rounded-lg overflow-hidden" id={`op-${slug(method + " " + path)}`}>
      <div className="bg-gray-50 px-4 py-2 flex flex-wrap items-center gap-2 border-b border-gray-200">
        <span className={`text-xs font-bold uppercase px-2 py-0.5 rounded text-white ${methodColor}`}>{method}</span>
        <code className="text-sm font-mono text-gray-800">{path}</code>
        {op.deprecated && <span className="text-xs font-semibold text-red-700 uppercase">deprecated</span>}
      </div>
      <div className="px-4 py-3">
        <p className="font-semibold mb-1">{op.summary}</p>
        <Paragraphs text={op.description} className="text-sm text-gray-700 my-1" />
        {parameters.length > 0 && (
          <table className="w-full text-sm my-3 border border-gray-200">
            <thead>
              <tr className="bg-gray-50 text-left text-gray-600">
                <th className="py-1 px-2 font-medium">Parameter</th>
                <th className="py-1 px-2 font-medium">In</th>
                <th className="py-1 px-2 font-medium">Type</th>
                <th className="py-1 px-2 font-medium">Description</th>
              </tr>
            </thead>
            <tbody>
              {parameters.map((p, i) => (
                <tr key={`${p.in}:${p.name}:${i}`} className="border-t border-gray-100 align-top">
                  <td className="py-1 px-2 whitespace-nowrap"><code>{p.name}</code>{p.required && <span className="text-red-700" title="required"> *</span>}</td>
                  <td className="py-1 px-2 text-gray-600">{p.in}</td>
                  <td className="py-1 px-2 text-gray-600 whitespace-nowrap">{schemaLabel(spec, p.schema)}</td>
                  <td className="py-1 px-2">
                    {p.description && <Inline text={p.description} />}
                    {p.schema?.default !== undefined && <span className="text-gray-500"> Default <code>{String(p.schema.default)}</code>.</span>}
                    {p.example !== undefined && <span className="text-gray-500"> Example <code>{Array.isArray(p.example) ? p.example.join(", ") : String(p.example)}</code>.</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {body && (
          <div className="text-sm my-3">
            <span className="font-medium">Request body</span> <span className="text-gray-600">({bodyType}{body.required ? ", required" : ""})</span>
            {body.description && <span>: <Inline text={body.description} /></span>}
            {bodyExample !== undefined && (
              <pre className="bg-gray-50 border border-gray-200 rounded p-2 mt-1 overflow-x-auto text-xs">{typeof bodyExample === "string" ? bodyExample : JSON.stringify(bodyExample, null, 2)}</pre>
            )}
          </div>
        )}
        <ul className="text-sm my-2">
          {responses.map(([status, r]) => (
            <li key={status}><code>{status}</code> <Inline text={r.description || ""} />{r.content && Object.keys(r.content)[0] !== "application/json" ? <span className="text-gray-500"> ({Object.keys(r.content)[0]})</span> : null}</li>
          ))}
        </ul>
        {example && <ApiExample method="GET" url={example.url} {...example.query} />}
        {method === "post" && bodyExample !== undefined && (
          <pre className="bg-gray-900 text-gray-100 rounded p-3 my-3 overflow-x-auto text-xs" data-testid="curl">{`curl -X POST "${(process.env.REACT_APP_APIURL || "").replace(/\/+$/, "")}${path}" \\\n  -H "Content-Type: ${bodyType}" \\\n  -d '${typeof bodyExample === "string" ? bodyExample.replace(/\n/g, "\\n") : JSON.stringify(bodyExample)}'`}</pre>
        )}
      </div>
    </div>
  );
}

/**
 * The API reference, rendered from the OpenAPI description the API serves:
 * every operation under its tag, with its parameters, and a "Try it" block
 * wherever the description gives an example to try.
 */
export default function OpenApiReference({ spec: given }: { spec?: any }) {
  const [spec, setSpec] = useState<any>(given ?? null);
  const [error, setError] = useState<any>(null);

  useEffect(() => {
    if (given) return;
    let cancelled = false;
    get<any>("api/v1/openapi.json")
      .then((s) => { if (!cancelled) setSpec(s); })
      .catch((e) => { if (!cancelled) setError(e); });
    return () => { cancelled = true; };
  }, [given]);

  if (error) return <ErrorMessage what="The API description" error={error} />;
  if (!spec) return <p className="text-gray-500">Loading the API description…</p>;

  const operations: { method: string; path: string; pathItem: any; op: any }[] = [];
  for (const [path, pathItem] of Object.entries<any>(spec.paths || {})) {
    for (const method of METHODS) {
      if (pathItem[method]) operations.push({ method, path, pathItem, op: pathItem[method] });
    }
  }
  const tagNames: string[] = (spec.tags || []).map((t: any) => t.name);
  for (const o of operations) for (const t of o.op.tags || []) if (!tagNames.includes(t)) tagNames.push(t);
  const tagDescription = (name: string) => (spec.tags || []).find((t: any) => t.name === name)?.description;

  return (
    <div className="openapi-reference">
      <Paragraphs text={spec.info?.description} className="my-3" />
      {tagNames.map((tag) => {
        const ops = operations.filter((o) => (o.op.tags || []).includes(tag));
        if (ops.length === 0) return null;
        return (
          <section key={tag} className="mt-8">
            <h2 id={`api-${slug(tag)}`} className="text-xl font-bold mt-6 mb-2">{tag}</h2>
            {tagDescription(tag) && <p className="text-gray-700"><Inline text={tagDescription(tag)} /></p>}
            {ops.map((o) => <Operation key={`${o.method} ${o.path}`} spec={spec} {...o} />)}
          </section>
        );
      })}
    </div>
  );
}
