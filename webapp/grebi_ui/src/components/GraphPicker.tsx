import { Select, MenuItem, FormControl } from "@mui/material";
import { Fragment, useEffect, useState } from "react";
import { get } from "../app/api";

export default function GraphPicker({
  graph,
  setGraph,
  compact,
}: {
  graph?: string | undefined;
  setGraph: (graph: string) => void
  compact?:boolean
}) {

    let [graphs, setGraphs] = useState<string[]>([]);
    let [stats, setStats] = useState<Record<string, { num_nodes: number; num_edges: number }> | null>(null);

    useEffect(() => {
        get<string[]>(`api/v1/graphs`).then(r => setGraphs(r));
        get<Record<string, { num_nodes: number; num_edges: number }>>("api/v1/stats").then(r => setStats(r));
    }, []);

    return (
    <FormControl variant="standard">
        <Select
            value={graph}
            onChange={(e) => setGraph(e.target.value)}
            size={compact ? "small" : "medium"}
            disableUnderline={compact}
        >
            {graphs.map((s) => (
                <MenuItem key={s} value={s}>
                    {s}
                    {stats && stats[s] && (
                        <span style={{ marginLeft: 8, fontSize: '0.8em', color: '#888' }}>
                            {stats[s].num_nodes.toLocaleString()} nodes, {stats[s].num_edges.toLocaleString()} edges
                        </span>
                    )}
                </MenuItem>
            ))}
        </Select>
    </FormControl>
);


}
