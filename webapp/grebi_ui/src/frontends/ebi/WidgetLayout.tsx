import React, { useEffect, useState } from "react";
import { Outlet, useLocation, useNavigate, useNavigationType } from "react-router-dom";
import urlJoin from "url-join";
import { EmbeddedContext } from "../../app/embedded";
import { siteUrl } from "../../app/instance";
import { RouteErrorBoundary } from "../../components/ErrorBoundary";

/**
 * The layout of a page inside a widget on another site (src/widgets): in
 * place of the site's header, a thin bar with a way back through the widget's
 * own history and a link to the same page on GrEBI; and the embedded context,
 * which tells the pages to leave out their breadcrumbs and the document title.
 */
export default function WidgetLayout() {
  const location = useLocation();
  const navigate = useNavigate();
  const navigationType = useNavigationType();

  // how far the widget has navigated from where it started: a push goes
  // deeper, a pop (Back) comes back out
  const [depth, setDepth] = useState(0);
  useEffect(() => {
    if (navigationType === "PUSH") {
      setDepth((d) => d + 1);
    } else if (navigationType === "POP") {
      setDepth((d) => Math.max(0, d - 1));
    }
  }, [location.key, navigationType]);

  const here = urlJoin(siteUrl(), location.pathname) + location.search + location.hash;

  return (
    <EmbeddedContext.Provider value={true}>
      <div className="relative" onClickCapture={openModifiedClicksOnSite} onAuxClickCapture={openModifiedClicksOnSite}>
        <div className="flex items-center gap-4 px-3 py-1.5 bg-black text-white text-sm">
          <a href={urlJoin(siteUrl(), "/")} target="_blank" rel="noopener noreferrer" className="flex items-center" title="GrEBI">
            <img src={urlJoin(siteUrl(), "/logo.svg")} alt="GrEBI" style={{ height: "22px" }} />
          </a>
          {depth > 0 && (
            <button type="button" className="hover:underline" onClick={() => navigate(-1)}>
              ← Back
            </button>
          )}
          <span className="flex-1" />
          <a className="hover:underline whitespace-nowrap" href={here} target="_blank" rel="noopener noreferrer">
            Open in GrEBI ↗
          </a>
        </div>
        <RouteErrorBoundary>
          <Outlet />
        </RouteErrorBoundary>
      </div>
    </EmbeddedContext.Provider>
  );
}

/**
 * The widget's links are the site's paths, which the router follows inside
 * the widget. A middle click or a modified click asks for a new tab, where
 * the browser would open the host's own site at that path; those open the
 * page on GrEBI instead.
 */
function openModifiedClicksOnSite(e: React.MouseEvent) {
  const modified = e.button === 1 || e.metaKey || e.ctrlKey || e.shiftKey;
  if (!modified || e.defaultPrevented) {
    return;
  }
  const target = e.target as Element;
  const anchor = target.closest ? (target.closest("a[href]") as HTMLAnchorElement | null) : null;
  const href = anchor ? anchor.getAttribute("href") : null;
  if (!href || !href.startsWith("/") || href.startsWith("//")) {
    return;
  }
  e.preventDefault();
  window.open(urlJoin(siteUrl(), href), "_blank", "noopener");
}
