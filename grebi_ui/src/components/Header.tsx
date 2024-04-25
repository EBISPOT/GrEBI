import { Link } from "react-router-dom";
import urlJoin from "url-join";
import { Helmet } from 'react-helmet';
import React, { Fragment } from "react";
import HomeIcon from '@mui/icons-material/Home';
import MediationIcon from '@mui/icons-material/Mediation';
import { Stack } from "@mui/material";
import JoinRightIcon from '@mui/icons-material/JoinRight';
import HelpIcon from '@mui/icons-material/Help';
import InfoIcon from '@mui/icons-material/Info';
import DownloadIcon from '@mui/icons-material/Download';


export default function Header({ section }: { section?: string }) {

  return (
    <header
      className="bg-black bg-right bg-cover"
      style={{
        // backgroundImage:
        //   "url('" +
        //   urlJoin(process.env.PUBLIC_URL!, "/embl-ebi-background-4.jpg") +
        //   "')",
      }}
    >
        <Helmet>
          <meta charSet="utf-8" />
          <title>{caps(section)} - GrEBI</title>
        </Helmet>
      <div className="container mx-auto px-4 flex flex-col md:flex-row md:gap-10">
        <div className="py-6 self-center">
          <a href={urlJoin(process.env.PUBLIC_URL!, "/")}>
            <img
              alt="OLS logo"
              className="h-8 inline-block"
              src={urlJoin(process.env.PUBLIC_URL!, "/logo.svg")}
            />
          </a>
        </div>
        <nav className="self-center">
          <ul
            className="bg-transparent text-white flex flex-wrap divide-white divide-x"
            data-description="navigational"
            role="menubar"
            data-dropdown-menu="6mg2ht-dropdown-menu"
          >
            <Link to="/">
              <li
                role="menuitem"
                className={`rounded-l-md px-4 py-3  ${
                  section === "home"
                    ? "bg-opacity-75 bg-neutral-500"
                    : "hover:bg-opacity-50 hover:bg-neutral-500"
                }`}
              >
                <Stack alignItems="center" direction="row" gap={1}>
                  <HomeIcon />
                  Home
                </Stack>
              </li>
            </Link>
            <Link to="/subgraphs">
              <li
                role="menuitem"
                className={`px-4 py-3 ${
                  section === "ontologies"
                    ? " bg-opacity-75 bg-neutral-500"
                    : "hover:bg-opacity-50 hover:bg-neutral-500 "
                }`}
              >
                <Stack alignItems="center" direction="row" gap={1}>
                  <JoinRightIcon />
                  Subgraphs
                </Stack>
              </li>
            </Link>
            <Link to="/datasources">
              <li
                role="menuitem"
                className={`px-4 py-3 ${
                  section === "ontologies"
                    ? " bg-opacity-75 bg-neutral-500"
                    : "hover:bg-opacity-50 hover:bg-neutral-500 "
                }`}
              >
                <Stack alignItems="center" direction="row" gap={1}>
                  <MediationIcon />
                  Datasources
                </Stack>
              </li>
            </Link>
            <Link to={`/help`}>
              <li
                role="menuitem"
                className={`px-4 py-3  ${
                  section === "help"
                    ? " bg-opacity-75 bg-neutral-500"
                    : "hover:bg-opacity-50 hover:bg-neutral-500"
                }`}
              >
                <Stack alignItems="center" direction="row" gap={1}>
                  <HelpIcon />
                  Help
                </Stack>
              </li>
            </Link>
            <Link to={`/about`}>
              <li
                role="menuitem"
                className={`px-4 py-3 ${
                  section === "about"
                    ? " bg-opacity-75 bg-neutral-500"
                    : "hover:bg-opacity-50 hover:bg-neutral-500"
                }`}
              >
                <Stack alignItems="center" direction="row" gap={1}>
                  <InfoIcon />
                  About
                </Stack>
              </li>
            </Link>
            <Link to={`/downloads`}>
              <li
                role="menuitem"
                className={`rounded-r-md px-4 py-3 ${
                  section === "downloads"
                    ? " bg-opacity-75 bg-neutral-500"
                    : "hover:bg-opacity-50 hover:bg-neutral-500"
                }`}
              >
                <Stack alignItems="center" direction="row" gap={1}>
                  <DownloadIcon />
                  Downloads
                </Stack>
              </li>
            </Link>
          </ul>
        </nav>
      </div>
    </header>
  );
}

function caps(str) {
    return str[0].toUpperCase() + str.slice(1);
}

