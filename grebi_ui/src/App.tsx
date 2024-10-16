
import React, { Fragment } from "react";
import {
  BrowserRouter,
  Route,
  Routes,
} from "react-router-dom";
import Footer from "./components/Footer";
import Error from "./pages/Error";
import Home from "./pages/home/Home";
import {Helmet} from "react-helmet";
import Search from "./pages/search/Search";
import NodePage from "./pages/node/NodePage";
import DownloadsPage from "./pages/downloads/Downloads";


import MuiThemeProvider from '@mui/styles/ThemeProvider'
import createTheme from '@mui/material/styles/createTheme'

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


class App extends React.Component {
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
          <Route path={`*`} element={<Error />} />
          <Route path={`/error`} element={<Error />} />

          <Route path={`/`} element={<Home />} />
          <Route path={`/subgraphs/:subgraph/search`} element={<Search />} />
          <Route path={`/subgraphs/:subgraph/nodes/:nodeId`} element={<NodePage />} />

          <Route path={`/downloads`} element={<DownloadsPage />} />
        </Routes>
        {/* <Footer /> */}
      </BrowserRouter>
      </Fragment>
      </MuiThemeProvider>
    );
  }
}

export default App;
