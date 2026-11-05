import React from "react";
import { Route, Routes } from "react-router-dom";
import EbiDownloadsPage from "./pages/EbiDownloadsPage";
import EbiErrorPage from "./pages/EbiErrorPage";
import EbiHomePage from "./pages/EbiHomePage";
import EbiNodePage from "./pages/EbiNodePage";
import EbiSearchPage from "./pages/EbiSearchPage";
import EbiTablesPage from "./pages/EbiTablesPage";
import EbiTablesHomePage from "./pages/EbiTablesHomePage";
import EbiQueryOrTopicPage from "./pages/EbiQueryOrTopicPage";
import EbiQueriesHomePage from "./pages/EbiQueriesHomePage";
import EbiDatasourcesPage from "./pages/EbiDatasourcesPage";
import EbiGraphPage from "./pages/EbiGraphPage";
import EbiEdgeSearchPage from "./pages/EbiEdgeSearchPage";
import EbiEdgePage from "./pages/EbiEdgePage";
import EbiLookupPage from "./pages/EbiLookupPage";
import DocsPage from "./pages/DocsPage";

/**
 * The pages and their paths, inside the given layout: the site's layout in
 * the app (EbiLayout), the widget's on other sites (WidgetLayout), the same
 * pages either way.
 */
export default function EbiRoutes({ layout }: { layout: React.ReactElement }) {
  return (
    <Routes>
      <Route element={layout}>
        <Route path={`*`} element={<EbiErrorPage />} />
        <Route path={`/error`} element={<EbiErrorPage />} />

        <Route path={`/`} element={<EbiHomePage />} />
        <Route path={`/graphs`} element={<EbiDatasourcesPage />} />
        <Route path={`/graphs/:graph`} element={<EbiGraphPage />} />
        <Route path={`/graphs/:graph/search`} element={<EbiSearchPage />} />
        <Route path={`/graphs/:graph/lookup`} element={<EbiLookupPage />} />
        <Route path={`/graphs/:graph/edges`} element={<EbiEdgeSearchPage />} />
        <Route path={`/graphs/:graph/edges/:edgeId`} element={<EbiEdgePage />} />
        <Route path={`/graphs/:graph/nodes/:nodeId`} element={<EbiNodePage />} />

        <Route path={`/tables`} element={<EbiTablesHomePage />} />
        <Route path={`/graphs/:graph/tables`} element={<EbiTablesHomePage />} />
        <Route path={`/graphs/:graph/tables/:queryid`} element={<EbiTablesPage />} />

        <Route path={`/graphs/:graph/queries`} element={<EbiQueriesHomePage />} />
        <Route path={`/graphs/:graph/queries/:queryid`} element={<EbiQueryOrTopicPage />} />

        <Route path={`/graphs/:graph/downloads`} element={<EbiDownloadsPage />} />

        <Route path={`/docs`} element={<DocsPage />} />
        <Route path={`/docs/*`} element={<DocsPage />} />
      </Route>
    </Routes>
  );
}
