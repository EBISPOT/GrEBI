import { useMemo, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import rehypeRaw from "rehype-raw";
import rehypeSlug from "rehype-slug";
import prismjs from "prismjs";
import "prismjs/components/prism-python";
import "prismjs/components/prism-bash";
import "prismjs/components/prism-cypher";
import "prismjs/components/prism-r";
import "prismjs/components/prism-json";
import "prismjs/components/prism-yaml";
import ApiExample from "./ApiExample";
import QueryTemplateExample from "./QueryTemplateExample";
import PubmedCitation from "./PubmedCitation";
import PubmedReferences from "./PubmedReferences";
import { resetPubmedRefs } from "./pubmedRegistry";

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      className="absolute top-2 right-2 px-2 py-1 text-xs rounded bg-gray-700 text-gray-200 hover:bg-gray-600 transition-colors"
      onClick={() => {
        navigator.clipboard.writeText(text).then(() => {
          setCopied(true);
          setTimeout(() => setCopied(false), 1500);
        });
      }}
    >
      {copied ? "Copied!" : "Copy"}
    </button>
  );
}

export default function DocsContent({
  markdown,
  images,
  seeAlso,
  onNavigate,
  sectionNumber,
}: {
  markdown: string;
  images: Record<string, string>;
  seeAlso?: Array<{ title: string; anchor: string }>;
  onNavigate?: (anchor: string) => void;
  sectionNumber?: number;
}) {
  // Build a map of slug -> number prefix by scanning headings in order.
  // This keeps rehype-slug IDs clean (matching sidebar anchors) while letting
  // heading components look up their number via props.id.
  const numberBySlug = useMemo(() => {
    const map = new Map<string, string>();
    if (sectionNumber == null) return map;
    let h2 = 0, h3 = 0;
    let inFence = false;
    for (const line of markdown.split('\n')) {
      if (/^```/.test(line)) { inFence = !inFence; continue; }
      if (inFence) continue;
      const m = line.match(/^(#{1,3}) (.+)$/);
      if (!m) continue;
      const level = m[1].length;
      const title = m[2];
      const slug = title.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
      if (level === 1) { h2 = 0; h3 = 0; map.set(slug, `${sectionNumber}`); }
      else if (level === 2) { h2++; h3 = 0; map.set(slug, `${sectionNumber}.${h2}`); }
      else if (level === 3) { h3++; map.set(slug, `${sectionNumber}.${h2}.${h3}`); }
    }
    return map;
  }, [markdown, sectionNumber]);

  // Pre-process: resolve image paths to data URIs only (numbers are rendered
  // in heading components so rehype-slug IDs stay clean and match sidebar anchors)
  const processed = useMemo(() => {
    return markdown.replace(
      /!\[([^\]]*)\]\(\.\/images\/([^)]+)\)/g,
      (_match, alt, filename) => {
        const dataUri = images[filename];
        if (dataUri) return `![${alt}](${dataUri})`;
        return _match;
      }
    );
  }, [markdown, images]);

  const components = useMemo(
    () => ({
      // Code blocks with copy button and syntax highlighting
      pre({ children, ...props }: any) {
        // Extract text content for the copy button
        const codeEl = children?.props;
        const text = codeEl?.children || "";
        return (
          <div className="relative group">
            <CopyButton text={typeof text === "string" ? text : ""} />
            <pre
              className="bg-gray-900 text-gray-100 rounded-lg p-4 overflow-x-auto text-sm my-4"
              {...props}
            >
              {children}
            </pre>
          </div>
        );
      },
      // Code blocks — PrismJS syntax highlighting (matches rest of app)
      code({ inline, children, className, ...props }: any) {
        if (inline) {
          return (
            <code
              className="bg-gray-100 text-pink-700 rounded px-1.5 py-0.5 font-mono"
              {...props}
            >
              {children}
            </code>
          );
        }
        const match = /language-(\w+)/.exec(className || "");
        const lang = match ? match[1] : "";
        const prismLang = lang === "curl" ? "bash" : lang;
        const grammar = prismjs.languages[prismLang];
        const text = String(children).replace(/\n$/, "");
        if (grammar) {
          return (
            <code
              className={`language-${prismLang}`}
              dangerouslySetInnerHTML={{ __html: prismjs.highlight(text, grammar, prismLang) }}
              {...props}
            />
          );
        }
        return (
          <code className={className} {...props}>
            {children}
          </code>
        );
      },
      // Links
      a({ href, children, ...props }: any) {
        return (
          <a
            href={href}
            className="text-blue-600 hover:underline"
            target={href && href.startsWith("#") ? undefined : "_blank"}
            rel={href && href.startsWith("#") ? undefined : "noopener noreferrer"}
            {...props}
          >
            {children}
          </a>
        );
      },
      // Tables — styled with Tailwind
      table({ children, ...props }: any) {
        return (
          <div className="overflow-x-auto my-4">
            <table
              className="min-w-full border-collapse border border-gray-200 text-sm"
              {...props}
            >
              {children}
            </table>
          </div>
        );
      },
      thead({ children, ...props }: any) {
        return (
          <thead className="bg-gray-100" {...props}>
            {children}
          </thead>
        );
      },
      th({ children, ...props }: any) {
        return (
          <th
            className="border border-gray-200 px-3 py-2 text-left font-medium"
            {...props}
          >
            {children}
          </th>
        );
      },
      td({ children, ...props }: any) {
        return (
          <td className="border border-gray-200 px-3 py-2" {...props}>
            {children}
          </td>
        );
      },
      // Images
      img({ src, alt, ...props }: any) {
        return (
          <img
            src={src}
            alt={alt}
            className="max-w-md my-4"
            {...props}
          />
        );
      },
      // Headings
      h1({ children, id, ...props }: any) {
        const num = id ? numberBySlug.get(id) : undefined;
        return (
          <h1 id={id} className="text-3xl font-bold mt-8 mb-4 pb-2 border-b-[3px] border-embl-purple-default text-embl-purple-default" {...props}>
            {num && <span className="mr-3">{num}</span>}{children}
          </h1>
        );
      },
      h2({ children, id, ...props }: any) {
        const num = id ? numberBySlug.get(id) : undefined;
        return (
          <h2 id={id} className="text-2xl font-semibold mt-6 mb-3 pb-2 border-b-2 border-embl-purple-default text-embl-purple-default" {...props}>
            {num && <span className="mr-3">{num}</span>}{children}
          </h2>
        );
      },
      h3({ children, id, ...props }: any) {
        const num = id ? numberBySlug.get(id) : undefined;
        return (
          <h3 id={id} className="text-xl font-semibold mt-5 mb-2 text-embl-purple-default" {...props}>
            {num && <span className="mr-2">{num}</span>}{children}
          </h3>
        );
      },
      h4({ children, id, ...props }: any) {
        return (
          <h4 id={id} className="text-lg font-semibold mt-4 mb-1 text-embl-purple-default" {...props}>
            {children}
          </h4>
        );
      },
      // Block quotes
      blockquote({ children, ...props }: any) {
        return (
          <blockquote
            className="border-l-4 border-blue-400 bg-blue-50 px-4 py-2 my-4 text-gray-700"
            {...props}
          >
            {children}
          </blockquote>
        );
      },
      // Paragraphs
      p({ children, ...props }: any) {
        return (
          <p className="my-3 leading-relaxed" {...props}>
            {children}
          </p>
        );
      },
      // Lists
      ul({ children, ...props }: any) {
        return (
          <ul className="list-disc list-inside my-3 space-y-1" {...props}>
            {children}
          </ul>
        );
      },
      ol({ children, ...props }: any) {
        return (
          <ol className="list-decimal list-inside my-3 space-y-1" {...props}>
            {children}
          </ol>
        );
      },
      // Custom HTML elements for interactive examples
      "api-example": (props: any) => <ApiExample {...props} />,
      "query-template": (props: any) => <QueryTemplateExample {...props} />,
      "pubmed": (props: any) => <PubmedCitation {...props} />,
    }),
    [numberBySlug]
  );

  // Reset pubmed ref numbering when markdown changes
  useMemo(() => resetPubmedRefs(), [markdown]);

  return (
    <article className="max-w-4xl">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        rehypePlugins={[rehypeRaw, rehypeSlug]}
        components={components}
      >
        {processed}
      </ReactMarkdown>
      <PubmedReferences />
      {seeAlso && seeAlso.length > 0 && (
        <nav className="mt-10 pt-4 border-t border-gray-200">
          <h3 className="text-sm font-semibold text-gray-500 uppercase tracking-wide mb-2">See also</h3>
          <ul className="list-none p-0 m-0 space-y-1">
            {seeAlso.map(({ title, anchor }) => (
              <li key={anchor}>
                <a
                  href={`#${anchor}`}
                  className="text-blue-600 hover:underline"
                  onClick={(e) => {
                    e.preventDefault();
                    onNavigate?.(anchor);
                  }}
                >
                  {title}
                </a>
              </li>
            ))}
          </ul>
        </nav>
      )}
    </article>
  );
}
