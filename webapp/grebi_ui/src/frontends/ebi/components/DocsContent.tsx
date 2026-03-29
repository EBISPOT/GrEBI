import { useMemo, useCallback, useState } from "react";
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
  onNavigate,
}: {
  markdown: string;
  images: Record<string, string>;
  onNavigate: (slug: string) => void;
}) {
  // Pre-process: resolve image paths to data URIs
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

  const resolveDocLink = useCallback(
    (href: string): { isDoc: boolean; slug: string } | null => {
      if (!href) return null;
      // Match ./foo.md or foo.md (relative doc links)
      const m = href.match(/^(?:\.\/)?([a-z0-9_-]+)\.md$/i);
      if (m) return { isDoc: true, slug: m[1] };
      return null;
    },
    []
  );

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
              className="bg-gray-100 text-pink-700 rounded px-1.5 py-0.5 text-sm font-mono"
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
      // Links — resolve internal doc links to SPA navigation
      a({ href, children, ...props }: any) {
        const docLink = resolveDocLink(href);
        if (docLink) {
          return (
            <a
              href={`/docs/${docLink.slug === "index" ? "" : docLink.slug}`}
              className="text-blue-600 hover:underline cursor-pointer"
              onClick={(e) => {
                e.preventDefault();
                onNavigate(docLink.slug);
              }}
              {...props}
            >
              {children}
            </a>
          );
        }
        return (
          <a
            href={href}
            className="text-blue-600 hover:underline"
            target="_blank"
            rel="noopener noreferrer"
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
      h1({ children, ...props }: any) {
        return (
          <h1 className="text-3xl font-bold mt-8 mb-4 pb-2 border-b border-gray-200" {...props}>
            {children}
          </h1>
        );
      },
      h2({ children, ...props }: any) {
        return (
          <h2 className="text-2xl font-semibold mt-6 mb-3" {...props}>
            {children}
          </h2>
        );
      },
      h3({ children, ...props }: any) {
        return (
          <h3 className="text-xl font-semibold mt-5 mb-2" {...props}>
            {children}
          </h3>
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
    }),
    [resolveDocLink, onNavigate]
  );

  return (
    <article className="max-w-4xl">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        rehypePlugins={[rehypeRaw, rehypeSlug]}
        components={components}
      >
        {processed}
      </ReactMarkdown>
    </article>
  );
}
