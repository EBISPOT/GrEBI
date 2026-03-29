import React, { useEffect, useState } from "react";
import {
  Dialog,
  DialogTitle,
  DialogContent,
  IconButton,
} from "@mui/material";
import { Close, ScatterPlot as ScatterPlotIcon, ArrowForward as ArrowForwardIcon } from "@mui/icons-material";
import { getPaginated } from "../../app/api";
import { difference } from "../../app/util";
import GraphEdge from "../../model/GraphEdge";
import GraphNodeRef from "../../model/GraphNodeRef";
import DatasourceSelector from "../DatasourceSelector";
import { DatasourceTags } from "../DatasourceTag";
import DataTable from "../datatable/DataTable";
import LoadingOverlay from "../LoadingOverlay";

export default function EdgeExpandDialog({
  open,
  onClose,
  onSelectNode,
  graph,
  nodeId,
  encodedNodeId,
  direction,
  edgeType,
  parentLabel,
}: {
  open: boolean;
  onClose: () => void;
  onSelectNode: (node: GraphNodeRef) => void;
  graph: string;
  nodeId: string;
  encodedNodeId: string;
  direction: "incoming" | "outgoing";
  edgeType: string;
  parentLabel?: string;
}) {
  const [loading, setLoading] = useState(true);
  const [edges, setEdges] = useState<GraphEdge[]>([]);
  const [total, setTotal] = useState(0);
  const [datasources, setDatasources] = useState<string[]>([]);
  const [dsEnabled, setDsEnabled] = useState<string[] | null>(null);

  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [filter, setFilter] = useState("");
  const [sortColumn, setSortColumn] = useState("grebi:type");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");

  useEffect(() => {
    if (!open) return;

    async function fetchEdges() {
      setLoading(true);
      try {
        const endpoint =
          direction === "incoming" ? "incoming_edge_refs" : "outgoing_edge_refs";
        const params = new URLSearchParams([
          ["page", String(page)],
          ["size", String(rowsPerPage)],
          ["sortBy", sortColumn],
          ["sortDir", sortDir],
          ["grebi:type", edgeType],
          ...(filter ? [["q", filter] as [string, string]] : []),
          ...(datasources.length > 0 && dsEnabled !== null
            ? difference(datasources, dsEnabled).map(
                (ds) => ["-grebi:datasources", ds] as [string, string]
              )
            : []),
        ]);

        const res = (
          await getPaginated<any>(
            `api/v1/graphs/${graph}/nodes/${encodedNodeId}/${endpoint}?${params}`
          )
        ).map((e) => new GraphEdge(e));

        setEdges(res.elements);
        setTotal(res.totalElements);

        if (res.facetFieldsToCounts["grebi:datasources"]) {
          const newDs = Object.keys(
            res.facetFieldsToCounts["grebi:datasources"]
          );
          setDatasources(newDs);
        }
      } catch (e) {
        console.error("Failed to load edges for expand dialog", e);
      } finally {
        setLoading(false);
      }
    }

    fetchEdges();
  }, [
    open,
    direction,
    edgeType,
    encodedNodeId,
    graph,
    page,
    rowsPerPage,
    filter,
    sortColumn,
    sortDir,
    dsEnabled ? JSON.stringify(dsEnabled) : null,
  ]);

  // Reset state when dialog opens with new params
  useEffect(() => {
    if (open) {
      setPage(0);
      setFilter("");
      setDsEnabled(null);
    }
  }, [open, edgeType, direction]);

  const handleSelectRow = (row: GraphEdge) => {
    // The "other" node: for incoming edges it's the "from" node, for outgoing it's the "to" node
    const selectedNode =
      direction === "incoming" ? row.getFrom() : row.getTo();
    onSelectNode(selectedNode);
    onClose();
  };

  const subjectName = parentLabel || nodeId;

  return (
    <Dialog open={open} onClose={onClose} maxWidth="lg" fullWidth>
      <DialogTitle
        sx={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: "6px", fontSize: "16px" }}>
          {direction === "incoming" ? (
            <>
              <ScatterPlotIcon sx={{ fontSize: 20, color: "#bbb" }} />
              <ArrowForwardIcon sx={{ fontSize: 18, color: "#888" }} />
              <span style={{ fontFamily: "monospace", fontSize: "14px", color: "#555" }}>{edgeType}</span>
              <ArrowForwardIcon sx={{ fontSize: 18, color: "#888" }} />
              <span style={{ fontWeight: "bold" }}>{subjectName}</span>
            </>
          ) : (
            <>
              <span style={{ fontWeight: "bold" }}>{subjectName}</span>
              <ArrowForwardIcon sx={{ fontSize: 18, color: "#888" }} />
              <span style={{ fontFamily: "monospace", fontSize: "14px", color: "#555" }}>{edgeType}</span>
              <ArrowForwardIcon sx={{ fontSize: 18, color: "#888" }} />
              <ScatterPlotIcon sx={{ fontSize: 20, color: "#bbb" }} />
            </>
          )}
        </div>
        <IconButton onClick={onClose} size="small">
          <Close />
        </IconButton>
      </DialogTitle>
      <DialogContent>
        {datasources.length > 0 && (
          <div className="pb-3">
            <DatasourceSelector
              datasources={datasources}
              dsEnabled={dsEnabled !== null ? dsEnabled : datasources}
              setDsEnabled={setDsEnabled}
            />
          </div>
        )}
        {loading && <LoadingOverlay message="Loading edges..." />}
        <DataTable
          columns={[
            {
              id: "grebi:datasources",
              name: "Datasources",
              selector: (row: GraphEdge) => (
                <DatasourceTags dss={row.getDatasources()} />
              ),
              sortable: true,
            },
            ...(direction === "incoming"
              ? [
                  {
                    id: "grebi:from",
                    name: "From Node",
                    selector: (row: GraphEdge) => (
                      <span>{row.getFrom().getName()}</span>
                    ),
                    sortable: true,
                  },
                ]
              : [
                  {
                    id: "grebi:to",
                    name: "To Node",
                    selector: (row: GraphEdge) => (
                      <span>{row.getTo().getName()}</span>
                    ),
                    sortable: true,
                  },
                ]),
            {
              id: "grebi:type",
              name: "Edge Type",
              selector: (row: GraphEdge) => <code>{row.getType()}</code>,
              sortable: true,
            },
          ]}
          defaultSelector={undefined}
          data={edges}
          dataCount={total}
          page={page}
          rowsPerPage={rowsPerPage}
          onRowsPerPageChange={(rpp) => {
            setRowsPerPage(rpp);
            setPage(0);
          }}
          onPageChange={setPage}
          onFilter={setFilter}
          onSelectRow={handleSelectRow}
          sortColumn={sortColumn}
          setSortColumn={setSortColumn}
          sortDir={sortDir}
          setSortDir={setSortDir}
        />
      </DialogContent>
    </Dialog>
  );
}
