import { useEffect, useState, useMemo, useCallback, useRef } from "react";
import { Link, useLocation } from "react-router-dom";
import DocsSidebar from "../components/DocsSidebar";
import DocsContent from "../components/DocsContent";

export interface SidebarEntry {
  title: string;
  anchor?: string;
  children?: SidebarEntry[];
}

export interface DocsPage {
  title: string;
  anchor: string;
  content: string;
}

export interface DocsManifest {
  sidebar: SidebarEntry[];
  pages: DocsPage[];
  images: Record<string, string>;
}

export default function DocsPage() {
  const [manifest, setManifest] = useState<DocsManifest | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [activeAnchor, setActiveAnchor] = useState<string>("");
  const [activePage, setActivePage] = useState<string>("");
  const location = useLocation();
  const contentRef = useRef<HTMLElement>(null);

  useEffect(() => {
    const base = (process.env.PUBLIC_URL || "").replace(/\/+$/, "");
    fetch(`${base}/docs-manifest.json`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((m) => {
        setManifest(m);
        // Set initial page from URL hash or default to first page
        const hash = location.hash.replace(/^#/, "");
        const initialPage = findPageForAnchor(m, hash) || (m.pages[0]?.anchor || "");
        setActivePage(initialPage);
        if (hash) setActiveAnchor(hash);
      })
      .catch((e) => setError(e.message));
  }, []);

  // Find which page owns a given anchor
  function findPageForAnchor(m: DocsManifest, anchor: string): string | null {
    if (!anchor) return null;
    // Check if the anchor is a page-level anchor
    for (const p of m.pages) {
      if (p.anchor === anchor) return p.anchor;
    }
    // Check sidebar: find which top-level section contains this anchor
    for (const entry of m.sidebar) {
      if (containsAnchor(entry, anchor)) return entry.anchor;
    }
    return null;
  }

  function containsAnchor(entry: SidebarEntry, anchor: string): boolean {
    if (entry.anchor === anchor) return true;
    return (entry.children || []).some(c => containsAnchor(c, anchor));
  }

  // Get current page content
  const currentPage = useMemo(() => {
    if (!manifest) return null;
    return manifest.pages.find(p => p.anchor === activePage) || manifest.pages[0] || null;
  }, [manifest, activePage]);

  // Scroll to anchor from URL hash on load / hash change
  useEffect(() => {
    if (!manifest) return;
    const hash = location.hash.replace(/^#/, "");
    if (hash) {
      const targetPage = findPageForAnchor(manifest, hash);
      if (targetPage && targetPage !== activePage) {
        setActivePage(targetPage);
      }
      requestAnimationFrame(() => {
        const el = document.getElementById(hash);
        if (el && contentRef.current) {
          const containerTop = contentRef.current.getBoundingClientRect().top;
          const elTop = el.getBoundingClientRect().top;
          contentRef.current.scrollTop += elTop - containerTop;
        }
      });
    }
  }, [manifest, location.hash]);

  // Track which heading is currently in view for sidebar highlighting
  useEffect(() => {
    if (!manifest) return;
    const allAnchors: string[] = [];
    function collect(entries: SidebarEntry[]) {
      for (const e of entries) {
        if (e.anchor) allAnchors.push(e.anchor);
        if (e.children) collect(e.children);
      }
    }
    collect(manifest.sidebar);

    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting) {
            setActiveAnchor(entry.target.id);
          }
        }
      },
      { rootMargin: "-80px 0px -70% 0px", threshold: 0 }
    );
    for (const anchor of allAnchors) {
      const el = document.getElementById(anchor);
      if (el) observer.observe(el);
    }
    return () => observer.disconnect();
  }, [manifest]);

  const scrollToAnchor = useCallback((anchor: string) => {
    const el = document.getElementById(anchor);
    if (el && contentRef.current) {
      const containerTop = contentRef.current.getBoundingClientRect().top;
      const elTop = el.getBoundingClientRect().top;
      contentRef.current.scrollTo({
        top: contentRef.current.scrollTop + elTop - containerTop,
        behavior: "smooth",
      });
    }
  }, []);

  const onNavigate = useCallback((anchor: string) => {
    if (!manifest) return;
    const targetPage = findPageForAnchor(manifest, anchor);
    const isPageAnchor = manifest.pages.some(p => p.anchor === anchor);
    if (targetPage && targetPage !== activePage) {
      setActivePage(targetPage);
      requestAnimationFrame(() => {
        requestAnimationFrame(() => {
          if (contentRef.current) contentRef.current.scrollTop = 0;
          if (!isPageAnchor) scrollToAnchor(anchor);
        });
      });
    } else if (isPageAnchor) {
      if (contentRef.current) contentRef.current.scrollTop = 0;
    } else {
      scrollToAnchor(anchor);
    }
    setActiveAnchor(anchor);
    window.history.replaceState(null, "", `#${anchor}`);
  }, [manifest, activePage, scrollToAnchor]);

  const sectionNumber = useMemo(() => {
    if (!manifest || !currentPage) return undefined;
    const idx = manifest.pages.findIndex(p => p.anchor === currentPage.anchor);
    return idx >= 0 ? idx + 1 : undefined;
  }, [manifest, currentPage]);

  const seeAlso = useMemo(() => {
    if (!manifest || !currentPage) return [];
    return manifest.pages
      .filter(p => p.anchor !== currentPage.anchor)
      .map(p => ({ title: p.title, anchor: p.anchor }));
  }, [manifest, currentPage]);

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

  return (
    <div className="flex overflow-hidden h-full">
      {/* Sidebar */}
      <DocsSidebar
        sidebar={manifest.sidebar}
        activeAnchor={activeAnchor}
        onNavigate={onNavigate}
      />
      {/* Content */}
      <main ref={contentRef} className="flex-1 min-w-0 px-8 py-6 overflow-y-auto h-full">
        <div className="flex items-start gap-2 mb-6 px-4 py-3 rounded-md bg-yellow-50 border border-yellow-300 text-yellow-900">
          <span className="font-bold shrink-0">Note:</span>
          <span>GrEBI is still in beta testing and its docs are a work in progress. Check back later for more progress, or follow our <Link className="link-default" to="https://github.com/EBISPOT/GrEBI/issues">GitHub issue tracker</Link> for updates.</span>
        </div>
        {currentPage ? (
          <DocsContent
            markdown={currentPage.content}
            images={manifest.images}
            seeAlso={seeAlso}
            onNavigate={onNavigate}
            sectionNumber={sectionNumber}
          />
        ) : (
          <p className="text-gray-500">No content found.</p>
        )}
      </main>
    </div>
  );
}
