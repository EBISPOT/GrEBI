import { useParams, useSearchParams } from "react-router-dom";
import MaterialisedQueryTable from "../../../components/matq/MaterialisedQueryTable";
import EbiBreadcrumbsBar from "../EbiBreadcrumbsBar";
import { useEffect, useState } from "react";
import { Box, Link, Typography } from "@mui/material";
import QueryTable from "../../../components/query/QueryTable";
import QueryFacets from "../../../components/query/QueryFacets";
import QueryTopic from "../../../model/QueryTopic";
import { get } from "../../../app/api";

export default function EbiTablesHomePage() {

  let params = useParams();
  let [searchParams, setSearchParams] = useSearchParams();
  let subgraph:string|undefined = params.subgraph
  let queryid:string|undefined = params.queryid

  const [topics, setTopics] = useState<QueryTopic[]|null>(null);
  const [selectedTopics, setSelectedTopics] = useState<Set<string>>(new Set());

  useEffect(() => {
    get<QueryTopic[]>(`api/v1/topics`).then(r => setTopics(r));
  }, []);

  if(!subgraph) {
    throw new Error("Subgraph is required");
  }
  
    return (
        <div>
        <EbiBreadcrumbsBar
            subgraph={subgraph}
            entries={[
              { url: `/graphs`, label: "Graphs" },
              { url: `/graphs/${subgraph}/queries`, label: "Queries" }
            ]}
        />
        <main className="container mx-auto px-4 h-fit pt-2">
        <div className="grid grid-cols-1 gap-8">
            <Typography variant="h4">Graph Queries</Typography>
            <p>
            The EBI KG can be queried using a selection of graph query templates, for which you can fill in your own parameters. Results are returned in a tabular format and can be downloaded as CSV. These query templates are also available as tools on the MCP server.</p>
            
            <Box sx={{ display: 'grid', gridTemplateColumns: '250px 1fr', gap: 3 }}>
              {topics && (
                <QueryFacets 
                  topics={topics}
                  selectedTopics={selectedTopics}
                  onSelectionChange={setSelectedTopics}
                />
              )}
              <QueryTable subgraph={subgraph} selectedTopics={selectedTopics} />
            </Box>
        </div>
        </main>
        </div>
    );
}
