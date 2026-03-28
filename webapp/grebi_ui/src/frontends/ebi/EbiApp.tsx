
import React, { Fragment } from "react";
import {
  BrowserRouter,
  Route,
  Routes,
} from "react-router-dom";
import {Helmet} from "react-helmet";

import MuiThemeProvider from '@mui/styles/ThemeProvider'
import createTheme from '@mui/material/styles/createTheme'
import EbiDownloadsPage from "./pages/EbiDownloadsPage";
import EbiErrorPage from "./pages/EbiErrorPage";
import EbiHomePage from "./pages/EbiHomePage";
import EbiNodePage from "./pages/EbiNodePage";
import EbiSearchPage from "./pages/EbiSearchPage";
import EbiTablesPage from "./pages/EbiTablesPage";
import EbiTablesHomePage from "./pages/EbiTablesHomePage";
import EbiQueryPage from "./pages/EbiQueryPage";
import EbiQueriesHomePage from "./pages/EbiQueriesHomePage";
import EbiDatasourcesPage from "./pages/EbiDatasourcesPage";
import EbiSubgraphPage from "./pages/EbiSubgraphPage";
import EbiEdgeSearchPage from "./pages/EbiEdgeSearchPage";
import EbiLayout from "./EbiLayout";

const theme = createTheme({
  palette: {
    primary: {
      main: '#ff0000',
    },
    secondary: {
      main: '#ff0000',
    }
  }
});


class EbiApp extends React.Component {
  render() {
    return (
       <MuiThemeProvider theme={theme}>
      <Fragment>
        <Helmet>
          <meta charSet="utf-8" />
          <title>GrEBI: Knowledge Graphs @ EMBL-EBI</title>
        </Helmet>
      <BrowserRouter basename={process.env.PUBLIC_URL!}>
        <Routes>
          <Route element={<EbiLayout />}>
            <Route path={`*`} element={<EbiErrorPage />} />
            <Route path={`/error`} element={<EbiErrorPage />} />

            <Route path={`/`} element={<EbiHomePage />} />
            <Route path={`/graphs`} element={<EbiDatasourcesPage />} />
            <Route path={`/graphs/:subgraph`} element={<EbiSubgraphPage />} />
            <Route path={`/graphs/:subgraph/search`} element={<EbiSearchPage />} />
            <Route path={`/graphs/:subgraph/edges`} element={<EbiEdgeSearchPage />} />
            <Route path={`/graphs/:subgraph/nodes/:nodeId`} element={<EbiNodePage />} />

            <Route path={`/tables`} element={<EbiTablesHomePage />} />
            {/* <Route path={`/results/:queryid`} element={<EbiResultsPage />} /> */}
            <Route path={`/graphs/:subgraph/tables`} element={<EbiTablesHomePage />} />
            <Route path={`/graphs/:subgraph/tables/:queryid`} element={<EbiTablesPage />} />

            <Route path={`/graphs/:subgraph/queries`} element={<EbiQueriesHomePage />} />
            <Route path={`/graphs/:subgraph/queries/:queryid`} element={<EbiQueryPage />} />

            <Route path={`/graphs/:subgraph/downloads`} element={<EbiDownloadsPage />} />
          </Route>
        </Routes>
        {/* <EbiFooter /> */}
      </BrowserRouter>
      </Fragment>
      </MuiThemeProvider>
    );
  }
}

export default EbiApp;

