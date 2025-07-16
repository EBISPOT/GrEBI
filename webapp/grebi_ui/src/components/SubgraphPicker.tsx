import { Select, MenuItem, InputLabel, FormControl } from "@mui/material";
import { Fragment, useEffect, useState } from "react";
import { get } from "../app/api";

export default function SubgraphPicker({
  subgraph,
  setSubgraph,
  compact
}: {
  subgraph?: string | undefined;
  setSubgraph: (subgraph: string) => void
  compact?:boolean
}) {

    let [subgraphs, setSubgraphs] = useState<string[]>([]);

    useEffect(() => {
        get<string[]>(`api/v1/subgraphs`).then(r => setSubgraphs(r));
    }, []);

    return (
    <FormControl variant="standard">
        <InputLabel 
            id="subgraph-label" 
            sx={{ textTransform: 'lowercase', fontVariant: 'small-caps' }}
        >
            Subgraph
        </InputLabel>
        <Select
            value={subgraph}
            labelId="subgraph-label"
            onChange={(e) => setSubgraph(e.target.value)}
            size={compact ? "small" : "medium"}
        >
            {subgraphs.map((s) => (
                <MenuItem key={s} value={s}>{s}</MenuItem>
            ))}
        </Select>
    </FormControl>
);


}
