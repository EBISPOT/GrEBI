
import React, { Fragment } from "react";
import { BrowserRouter } from "react-router-dom";
import { Helmet } from "react-helmet";

import { ThemeProvider as MuiThemeProvider } from '@mui/material/styles'
import EbiLayout from "./EbiLayout";
import EbiRoutes from "./EbiRoutes";
import { theme } from "./theme";


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
        <EbiRoutes layout={<EbiLayout />} />
        {/* <EbiFooter /> */}
      </BrowserRouter>
      </Fragment>
      </MuiThemeProvider>
    );
  }
}

export default EbiApp;

