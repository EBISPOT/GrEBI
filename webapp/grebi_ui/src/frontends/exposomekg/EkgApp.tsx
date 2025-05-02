

import React, { Fragment } from "react";
import {
  BrowserRouter,
  Route,
  Routes,
} from "react-router-dom";
import {Helmet} from "react-helmet";

import MuiThemeProvider from '@mui/styles/ThemeProvider'
import createTheme from '@mui/material/styles/createTheme'
import EkgErrorPage from "./pages/EkgErrorPage";
import EkgHomePage from "./pages/EkgHomePage";
import EkgNodePage from "./pages/EkgNodePage";
import EkgSearchPage from "./pages/EkgSearchPage";
import EkgDownloadsPage from "./pages/EkgDownloadsPage";

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


class EkgApp extends React.Component {
  render() {
    return (
       <MuiThemeProvider theme={theme}>
      <Fragment>
        <Helmet>
          <meta charSet="utf-8" />
          <title>EMBL HETT ExposomeKG</title>
        </Helmet>
      <BrowserRouter basename={process.env.PUBLIC_URL!}>
        <Routes>
          <Route path={`*`} element={<EkgErrorPage />} />
          <Route path={`/error`} element={<EkgErrorPage />} />

          <Route path={`/`} element={<EkgHomePage />} />
          <Route path={`/search`} element={<EkgSearchPage />} />
          <Route path={`/nodes/:nodeId`} element={<EkgNodePage />} />

          <Route path={`/downloads`} element={<EkgDownloadsPage />} />
        </Routes>
        {/* <EkgFooter /> */}
      </BrowserRouter>
      </Fragment>
      </MuiThemeProvider>
    );
  }
}

export default EkgApp;

