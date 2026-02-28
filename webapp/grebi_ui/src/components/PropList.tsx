import React, { Fragment } from "react";
import { Grid } from "@mui/material";
import PropVals from "./node_prop_table/PropVals";
import PropLabel from "./node_prop_table/PropLabel";
import PropVal from "../model/PropVal";
import Refs from "../model/Refs";

const HIDDEN_KEYS = new Set([
  "_refs",
  "grebi:edgeId",
  "grebi:fromNodeId",
  "grebi:toNodeId",
  "grebi:nodeId",
  "from",
  "to",
]);

export default function PropList(params: {
  subgraph: string;
  refs: Refs;
  props: { [key: string]: PropVal[] };
}) {
  let { subgraph, refs, props } = params;

  let propKeys = Object.keys(props).filter((k) => !HIDDEN_KEYS.has(k));

  return (
    <Grid container spacing={1} direction="row">
      {propKeys.map((key) => (
        <Fragment key={key}>
          <Grid
            item
            xs={12}
            style={{ overflow: "hidden", padding: "8px" }}
            className="bg-gradient-to-r from-neutral-light to-white rounded-lg"
          >
            <PropLabel prop={key} refs={refs} />
          </Grid>
          <Grid item xs={12} style={{ padding: "8px" }}>
            <div className="pl-2 mb-2">
              <PropVals subgraph={subgraph} refs={refs} values={props[key]} />
            </div>
          </Grid>
        </Fragment>
      ))}
    </Grid>
  );
}
