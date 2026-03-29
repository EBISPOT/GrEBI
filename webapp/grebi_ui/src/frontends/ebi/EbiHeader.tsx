import { Link, useLocation, useNavigate } from "react-router-dom";
import urlJoin from "url-join";
import { Helmet } from 'react-helmet';
import { Stack } from "@mui/material";
import DownloadIcon from '@mui/icons-material/Download';
import TravelExplore from '@mui/icons-material/TravelExplore';
import Apps from '@mui/icons-material/Apps';
import { ManageSearch, TableChart } from "@mui/icons-material";
import Breadcrumbs, { BreadcrumbsEntry } from "../../components/Breadcrumbs";

export default function EbiHeader({
  section,
  graph,
  showBreadcrumbsBar,
  breadcrumbs
}: {
  section?: string,
  graph?: string,
  showBreadcrumbsBar?: boolean,
  breadcrumbs?: BreadcrumbsEntry[]
}) {
  let loc = useLocation();
  let navigate = useNavigate();

  // Persist last-selected graph so nav items work even on /graphs
  if (graph) {
    sessionStorage.setItem("grebi_last_graph", graph);
  }
  const effectiveGraph = graph || sessionStorage.getItem("grebi_last_graph") || undefined;

function setGraph(graph: string) {
  const { pathname, search, hash } = loc; 
  const newPath = pathname.replace(/graphs\/[^/]+/, `graphs/${graph}`);
  const newUrl = `${newPath}${search}${hash}`;
  navigate(newUrl);
}


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
            <Link to={`/`}>
              <li
                role="menuitem"
                className={`rounded-l-md px-4 py-3  ${
                  section === "home" || section === "explore" || section === "search"
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
            <Link to={`/graphs`}>
              <li
                role="menuitem"
                className={`px-4 py-3 ${
                  section === "graphs" || section === "about"
                    ? " bg-opacity-30 bg-neutral-500"
                    : "hover:bg-opacity-50 hover:bg-neutral-500 "
                }`}
              >
                <Stack alignItems="center" direction="row" gap={1}>
                  <Apps />
                  Graphs
                </Stack>
              </li>
            </Link>
            <Link to={effectiveGraph ? `/graphs/${effectiveGraph}/queries` : `/graphs`}>
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
            <Link to={effectiveGraph ? `/graphs/${effectiveGraph}/tables` : `/graphs`}>
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
            <Link to={effectiveGraph ? `/graphs/${effectiveGraph}/downloads` : `/graphs`}>
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
  showBreadcrumbsBar && (
    <div className="bg-stone-100 pt-1 pl-2 pr-2 pb-1 flex flex-row justify-between items-center">
      
    {breadcrumbs !== undefined 
      ? <Breadcrumbs graph={graph} setGraph={setGraph} entries={breadcrumbs} /> 
      : <div />}
    </div>
  )
}

    </header>
  );
}

function caps(str) {
    return str[0].toUpperCase() + str.slice(1);
}

