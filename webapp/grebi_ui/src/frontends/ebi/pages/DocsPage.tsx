import { useEffect, useState, useMemo, useCallback } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import DocsSidebar from "../components/DocsSidebar";
import DocsContent from "../components/DocsContent";

export interface SidebarEntry {
  title: string;
  path?: string;
  children?: SidebarEntry[];
}

export interface DocsManifest {
  sidebar: SidebarEntry[];
  pages: Record<string, string>;
  images: Record<string, string>;
}

export default function DocsPage() {
  const [manifest, setManifest] = useState<DocsManifest | null>(null);
  const [error, setError] = useState<string | null>(null);
  const location = useLocation();
  const navigate = useNavigate();

  // Current page slug derived from URL: /docs/foo → "foo", /docs → "index"
  const slug = useMemo(() => {
    const parts = location.pathname.replace(/^\/docs\/?/, "").replace(/\/$/, "");
    return parts || "index";
  }, [location.pathname]);

  useEffect(() => {
    const base = (process.env.PUBLIC_URL || "").replace(/\/+$/, "");
    fetch(`${base}/docs-manifest.json`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((m) => setManifest(m))
      .catch((e) => setError(e.message));
  }, []);

  const onNavigate = useCallback(
    (targetSlug: string) => {
      navigate(`/docs/${targetSlug === "index" ? "" : targetSlug}`);
    },
    [navigate]
  );

  if (error) {
    return (
      <main className="container mx-auto px-4 py-8">
        <p className="text-red-600">Failed to load documentation: {error}</p>
      </main>
    );
  }

  if (!manifest) {
    return (
      <main className="container mx-auto px-4 py-8">
        <p className="text-gray-500">Loading documentation…</p>
      </main>
    );
  }

  const markdown = manifest.pages[slug];

  return (
    <div className="flex" style={{ minHeight: "calc(100vh - 120px)" }}>
      {/* Sidebar */}
      <DocsSidebar
        sidebar={manifest.sidebar}
        activeSlug={slug}
        onNavigate={onNavigate}
      />
      {/* Content */}
      <main className="flex-1 min-w-0 px-8 py-6 overflow-auto">
        {markdown ? (
          <DocsContent
            markdown={markdown}
            images={manifest.images}
            onNavigate={onNavigate}
          />
        ) : (
          <p className="text-gray-500">
            Page <code>{slug}</code> not found.
          </p>
        )}
      </main>
    </div>
  );
}
