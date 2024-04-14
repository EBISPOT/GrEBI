
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

class App extends React.Component {
  render() {
    return (
      <Fragment>
        <Helmet>
          <meta charSet="utf-8" />
          <title>GrEBI</title>
        </Helmet>
      <BrowserRouter basename={process.env.PUBLIC_URL!}>
        <Routes>
          <Route path={`*`} element={<Error />} />
          <Route path={`/error`} element={<Error />} />

          <Route path={`/`} element={<Home />} />
        </Routes>
        <Footer />
      </BrowserRouter>
      </Fragment>
    );
  }
}

export default App;
