import { useParams } from "react-router-dom";
import EbiBreadcrumbsBar from "../EbiBreadcrumbsBar";
import { Typography } from "@mui/material";
import ResultsTable from "../../../components/matq/ResultsTable";
import { tableFileUrl } from "../../../app/ftp";
import { Download } from "@mui/icons-material";

export default function EbiTablesPage() {

  let params = useParams();
  let graph:string|undefined = params.graph
  let queryid:string|undefined = params.queryid

  if(!graph || !queryid) {
    throw new Error("??");
  }

    return (
        <div>
        <EbiBreadcrumbsBar graph={graph} entries={[
          { url: `/graphs`, label: "Graphs" },
          { url: `/graphs/${graph}/tables`, label: "Tables" },
          { url: `/graphs/${graph}/tables/${queryid}`, label: <code>{queryid}</code> }
        ]} />
        <main className="container mx-auto px-4 h-fit pt-2">
        <div className="grid grid-cols-2 lg:grid-cols-1 lg:gap-8">
            <Typography variant="h4">{queryid}</Typography>
            <p>
              <a className="link-default inline-flex items-center gap-1 mr-6" href={tableFileUrl(graph, queryid, "csv")} target="_blank" rel="noopener noreferrer">
                <Download fontSize="small" /> Download the whole table as CSV
              </a>
              <a className="link-default inline-flex items-center gap-1" href={tableFileUrl(graph, queryid, "parquet")} target="_blank" rel="noopener noreferrer">
                <Download fontSize="small" /> as Parquet
              </a>
            </p>
            <ResultsTable graph={graph} queryid={queryid} />
        </div>
        </main>
        </div>
    );
}

