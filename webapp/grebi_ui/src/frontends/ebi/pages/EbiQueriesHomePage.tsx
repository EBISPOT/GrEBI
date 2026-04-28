import { Link as RouterLink, useParams } from "react-router-dom";
import EbiBreadcrumbsBar from "../EbiBreadcrumbsBar";
import { useEffect, useMemo, useState } from "react";
import { Box, CircularProgress, Stack, Typography } from "@mui/material";
import { MenuBook } from "@mui/icons-material";
import QueryTable from "../../../components/query/QueryTable";
import QueryFacets from "../../../components/query/QueryFacets";
import {
  filterQueryTemplates,
  getAvailableQueryTemplateInputs,
  getAvailableQueryTemplateOutputs,
} from "../../../components/query/queryTemplateFilters";
import QueryTopic from "../../../model/QueryTopic";
import { QueryTemplate } from "../../../model/QueryTemplate";
import { get } from "../../../app/api";

export default function EbiQueriesHomePage() {
  const params = useParams();
  const graph: string | undefined = params.graph;

  const [topics, setTopics] = useState<QueryTopic[] | null>(null);
  const [queries, setQueries] = useState<QueryTemplate[] | null>(null);
  const [selectedTopics, setSelectedTopics] = useState<Set<string>>(new Set());
  const [selectedInputs, setSelectedInputs] = useState<Set<string>>(new Set());
  const [selectedOutputs, setSelectedOutputs] = useState<Set<string>>(new Set());

  useEffect(() => {
    get<QueryTopic[]>(`api/v1/topics`).then((response) => setTopics(response));
  }, []);

  useEffect(() => {
    if (!graph) {
      return;
    }
    get<QueryTemplate[]>(`api/v1/graphs/${graph}/query_templates`).then((response) => setQueries(response));
  }, [graph]);

  useEffect(() => {
    setSelectedTopics(new Set());
    setSelectedInputs(new Set());
    setSelectedOutputs(new Set());
  }, [graph]);

  const availableInputs = useMemo(() => {
    return queries ? getAvailableQueryTemplateInputs(queries) : [];
  }, [queries]);

  const availableOutputs = useMemo(() => {
    return queries ? getAvailableQueryTemplateOutputs(queries) : [];
  }, [queries]);

  const filteredQueries = useMemo(() => {
    if (!queries) {
      return null;
    }
    return filterQueryTemplates(queries, {
      selectedTopics,
      selectedInputs,
      selectedOutputs,
    });
  }, [queries, selectedInputs, selectedOutputs, selectedTopics]);

  if (!graph) {
    throw new Error("Graph is required");
  }

  return (
    <div>
      <EbiBreadcrumbsBar
        graph={graph}
        entries={[
          { url: `/graphs`, label: "Graphs" },
          { url: `/graphs/${graph}/queries`, label: "Queries" },
        ]}
      />
      <main className="container mx-auto px-4 h-fit pt-2">
        <div className="grid grid-cols-1 gap-8">
          <Typography variant="h4">Graph Queries</Typography>
          <p>
            The EBI KG can be queried using a selection of graph query templates, for which you can fill in your own
            parameters. Results are returned in a tabular format and can be downloaded as CSV. These query templates
            are also available as tools on the MCP server.
          </p>
          <RouterLink to="/docs/queries" style={{ textDecoration: "none" }}>
            <Stack
              direction="row"
              alignItems="center"
              gap={0.5}
              sx={{ color: "#1976d2", fontSize: "0.95rem", "&:hover": { textDecoration: "underline" } }}
            >
              <MenuBook fontSize="small" />
              Learn more about GrEBI queries &raquo;
            </Stack>
          </RouterLink>

          <Box sx={{ display: "grid", gridTemplateColumns: { xs: "1fr", md: "250px 1fr" }, gap: 3 }}>
            {topics && queries ? (
              <QueryFacets
                topics={topics}
                availableInputs={availableInputs}
                availableOutputs={availableOutputs}
                selectedTopics={selectedTopics}
                selectedInputs={selectedInputs}
                selectedOutputs={selectedOutputs}
                onTopicsChange={setSelectedTopics}
                onInputsChange={setSelectedInputs}
                onOutputsChange={setSelectedOutputs}
              />
            ) : (
              <Box sx={{ display: "flex", justifyContent: "center", py: 4 }}>
                <CircularProgress />
              </Box>
            )}
            <QueryTable
              graph={graph}
              queries={filteredQueries}
              availableInputs={availableInputs}
              availableOutputs={availableOutputs}
              selectedInputs={selectedInputs}
              selectedOutputs={selectedOutputs}
              onInputsChange={setSelectedInputs}
              onOutputsChange={setSelectedOutputs}
            />
          </Box>
        </div>
      </main>
    </div>
  );
}
