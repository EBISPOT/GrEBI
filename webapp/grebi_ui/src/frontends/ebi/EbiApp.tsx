
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
          <title>EMBL-EBI Knowledge Graph</title>
        </Helmet>
      <BrowserRouter basename={process.env.PUBLIC_URL!}>
        <Routes>
          <Route path={`*`} element={<EbiErrorPage />} />
          <Route path={`/error`} element={<EbiErrorPage />} />

          <Route path={`/`} element={<EbiHomePage />} />
          <Route path={`/subgraphs/:subgraph`} element={<EbiHomePage />} />
          <Route path={`/subgraphs/:subgraph/search`} element={<EbiSearchPage />} />
          <Route path={`/subgraphs/:subgraph/nodes/:nodeId`} element={<EbiNodePage />} />

          <Route path={`/tables`} element={<EbiTablesHomePage />} />
          {/* <Route path={`/results/:queryid`} element={<EbiResultsPage />} /> */}
          <Route path={`/subgraphs/:subgraph/tables`} element={<EbiTablesHomePage />} />
          <Route path={`/subgraphs/:subgraph/tables/:queryid`} element={<EbiTablesPage />} />


          <Route path={`/subgraphs/:subgraph/queries`} element={<EbiQueriesHomePage />} />
          <Route path={`/subgraphs/:subgraph/queries/:queryid`} element={<EbiQueryPage />} />



          <Route path={`/subgraphs/:subgraph/downloads`} element={<EbiDownloadsPage />} />
        </Routes>
        {/* <EbiFooter /> */}
      </BrowserRouter>
      </Fragment>
      </MuiThemeProvider>
    );
  }
}

export default EbiApp;

