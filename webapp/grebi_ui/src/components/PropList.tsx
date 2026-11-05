import React from "react";
import PropVals from "./node_prop_table/PropVals";
import PropLabel from "./node_prop_table/PropLabel";
import { PropGrid, PropGridRow } from "./node_prop_table/PropGrid";
import PropVal from "../model/PropVal";
import Refs from "../model/Refs";
import { isHiddenProp } from "../model/hiddenProps";

export default function PropList(params: {
  graph: string;
  refs: Refs;
  props: { [key: string]: PropVal[] };
}) {
  let { graph, refs, props } = params;

  let propKeys = Object.keys(props).filter((k) => !isHiddenProp(k));

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
