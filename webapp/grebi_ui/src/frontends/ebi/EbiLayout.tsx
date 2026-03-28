import { Outlet, useLocation, Link } from "react-router-dom";
import urlJoin from "url-join";
import { Helmet } from "react-helmet";
import { Stack } from "@mui/material";
import DownloadIcon from "@mui/icons-material/Download";
import TravelExplore from "@mui/icons-material/TravelExplore";
import Apps from "@mui/icons-material/Apps";
import { ManageSearch, TableChart } from "@mui/icons-material";

export default function EbiLayout() {
  const loc = useLocation();
  const pathname = loc.pathname;

  const sgMatch = pathname.match(/^\/graphs\/([^/]+)(\/.*)?$/);
  const subgraph = sgMatch?.[1];
  const subpath = sgMatch?.[2] || "";

  // Persist last-selected subgraph so nav items work even on /graphs
  if (subgraph) {
    sessionStorage.setItem("grebi_last_subgraph", subgraph);
  }
  const effectiveSubgraph =
    subgraph || sessionStorage.getItem("grebi_last_subgraph") || undefined;

  let activeNav = "explore";
  if (pathname === "/graphs" || (subgraph && subpath === "")) {
    activeNav = "graphs";
  } else if (subpath.startsWith("/queries")) {
    activeNav = "queries";
  } else if (subpath.startsWith("/tables")) {
    activeNav = "tables";
  } else if (subpath.startsWith("/downloads")) {
    activeNav = "downloads";
  }

  const navTitles: Record<string, string> = {
    explore: "Explore",
    graphs: "Graphs",
    queries: "Queries",
    tables: "Tables",
    downloads: "Downloads",
  };

  return (
    <>
      <header className="bg-black bg-right bg-cover">
        <Helmet>
          <meta charSet="utf-8" />
          <title>{navTitles[activeNav] || "GrEBI"} - GrEBI</title>
        </Helmet>
        <div className="container mx-auto px-4 flex flex-col md:flex-row md:gap-10">
          <div className="py-6 self-center">
            <a href={urlJoin(process.env.PUBLIC_URL!, "/")}>
              <img
                style={{ height: "80px" }}
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
            >
              <Link to={`/`}>
                <li
                  role="menuitem"
                  className={`rounded-l-md px-4 py-3 ${
                    activeNav === "explore"
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
                    activeNav === "graphs"
                      ? "bg-opacity-30 bg-neutral-500"
                      : "hover:bg-opacity-50 hover:bg-neutral-500"
                  }`}
                >
                  <Stack alignItems="center" direction="row" gap={1}>
                    <Apps />
                    Graphs
                  </Stack>
                </li>
              </Link>
              <Link
                to={
                  effectiveSubgraph
                    ? `/graphs/${effectiveSubgraph}/queries`
                    : `/graphs`
                }
              >
                <li
                  role="menuitem"
                  className={`px-4 py-3 ${
                    activeNav === "queries"
                      ? "bg-opacity-30 bg-neutral-500"
                      : "hover:bg-opacity-50 hover:bg-neutral-500"
                  }`}
                >
                  <Stack alignItems="center" direction="row" gap={1}>
                    <ManageSearch />
                    Queries
                  </Stack>
                </li>
              </Link>
              <Link
                to={
                  effectiveSubgraph
                    ? `/graphs/${effectiveSubgraph}/tables`
                    : `/graphs`
                }
              >
                <li
                  role="menuitem"
                  className={`px-4 py-3 ${
                    activeNav === "tables"
                      ? "bg-opacity-30 bg-neutral-500"
                      : "hover:bg-opacity-50 hover:bg-neutral-500"
                  }`}
                >
                  <Stack alignItems="center" direction="row" gap={1}>
                    <TableChart />
                    Tables
                  </Stack>
                </li>
              </Link>
              <Link
                to={
                  effectiveSubgraph
                    ? `/graphs/${effectiveSubgraph}/downloads`
                    : `/graphs`
                }
              >
                <li
                  role="menuitem"
                  className={`rounded-r-md px-4 py-3 ${
                    activeNav === "downloads"
                      ? "bg-opacity-30 bg-neutral-500"
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
      <Outlet />
    </>
  );
}
