import { Select, MenuItem, FormControl } from "@mui/material";
import { Fragment, useEffect, useState } from "react";
import { get } from "../app/api";

export default function SubgraphPicker({
  subgraph,
  setSubgraph,
  compact,
}: {
  subgraph?: string | undefined;
  setSubgraph: (subgraph: string) => void
  compact?:boolean
}) {

    let [subgraphs, setSubgraphs] = useState<string[]>([]);
    let [stats, setStats] = useState<Record<string, { num_nodes: number; num_edges: number }> | null>(null);

    useEffect(() => {
        get<string[]>(`api/v1/subgraphs`).then(r => setSubgraphs(r));
        get<Record<string, { num_nodes: number; num_edges: number }>>("api/v1/stats").then(r => setStats(r));
    }, []);

    return (
    <FormControl variant="standard">
        <Select
            value={subgraph}
            onChange={(e) => setSubgraph(e.target.value)}
            size={compact ? "small" : "medium"}
            disableUnderline={compact}
        >
            {subgraphs.map((s) => (
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
