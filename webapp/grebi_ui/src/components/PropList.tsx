import React from "react";
import PropVals from "./node_prop_table/PropVals";
import PropLabel from "./node_prop_table/PropLabel";
import { PropGrid, PropGridRow } from "./node_prop_table/PropGrid";
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
  graph: string;
  refs: Refs;
  props: { [key: string]: PropVal[] };
}) {
  let { graph, refs, props } = params;

  let propKeys = Object.keys(props).filter((k) => !HIDDEN_KEYS.has(k));

  return (
    <PropGrid>
      {propKeys.map((key) => (
        <PropGridRow key={key} label={<PropLabel prop={key} refs={refs} />}>
          <PropVals graph={graph} refs={refs} values={props[key]} />
        </PropGridRow>
      ))}
    </PropGrid>
  );
}
