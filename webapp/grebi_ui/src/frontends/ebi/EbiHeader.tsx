import { Link } from "react-router-dom";
import urlJoin from "url-join";
import { Helmet } from 'react-helmet';
import React, { Fragment } from "react";
import HomeIcon from '@mui/icons-material/Home';
import MediationIcon from '@mui/icons-material/Mediation';
import { List, Stack } from "@mui/material";
import JoinRightIcon from '@mui/icons-material/JoinRight';
import HelpIcon from '@mui/icons-material/Help';
import InfoIcon from '@mui/icons-material/Info';
import DownloadIcon from '@mui/icons-material/Download';
import TravelExplore from '@mui/icons-material/TravelExplore';
import { FeaturedPlayList, Hub, LibraryBooks, ManageSearch, Polyline, Science, Search, Share, SnippetFolder, TableChart, ViewList } from "@mui/icons-material";
import SubgraphPicker from "../../components/SubgraphPicker";


export default function EbiHeader({
  section,
  subgraph,
  showBreadcrumbs
}: {
  section?: string,
  subgraph: string,
  showBreadcrumbs?: boolean,
}) {
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
              style={{height:'80px'}}
              alt="GrEBI logo"
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
                    ? "bg-opacity-30 bg-neutral-500"
                    : "hover:bg-opacity-50 hover:bg-neutral-500"
                }`}
              >
                <Stack alignItems="center" direction="row" gap={1}>
                  <TravelExplore />
                  Explore
                </Stack>
              </li>
            </Link>
            <Link to={`/subgraphs/${subgraph}/queries`}>
              <li
                role="menuitem"
                className={`px-4 py-3 ${
                  section === "queries"
                    ? " bg-opacity-30 bg-neutral-500"
                    : "hover:bg-opacity-50 hover:bg-neutral-500 "
                }`}
              >
                <Stack alignItems="center" direction="row" gap={1}>
                  <ManageSearch />
                  Queries
                </Stack>
              </li>
            </Link>
            <Link to={`/subgraphs/${subgraph}/tables`}>
              <li
                role="menuitem"
                className={`px-4 py-3 ${
                  section === "tables"
                    ? " bg-opacity-30 bg-neutral-500"
                    : "hover:bg-opacity-50 hover:bg-neutral-500 "
                }`}
              >
                <Stack alignItems="center" direction="row" gap={1}>
                  <TableChart />
                  Tables
                </Stack>
              </li>
            </Link>
            <Link to={`/downloads`}>
              <li
                role="menuitem"
                className={`rounded-r-md px-4 py-3 ${
                  section === "downloads"
                    ? " bg-opacity-30 bg-neutral-500"
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
        {
          showBreadcrumbs &&
          <div className="bg-stone-100 pt-1 pl-1 pr-2 pl-2 pb-1 flex flex-row justify-end">
            <SubgraphPicker 
              subgraph={subgraph}
              setSubgraph={setSubgraph}
              compact
            />
          </div>
        }
    </header>
  );
}

function caps(str) {
    return str[0].toUpperCase() + str.slice(1);
}

function setSubgraph(subgraph: string) {
  let currentUrl = window.location.href;
  let newUrl = currentUrl.replace(/subgraphs\/[^/]+/, `subgraphs/${subgraph}`);
  window.history.pushState({}, '', newUrl);
  window.location.reload();
}

