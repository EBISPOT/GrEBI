import { Checkbox, IconButton } from "@mui/material"
import { CheckBoxOutlineBlank, CheckBox as CheckBoxIcon } from "@mui/icons-material";
import React from "react";

export default function DatasourceSelector({
    datasources,
    dsEnabled, setDsEnabled,
    onMouseoverDs, onMouseoutDs,
    orientation = "horizontal",
}:{
    datasources:string[], 
    dsEnabled:string[], setDsEnabled:(ds:string[])=>void,
    onMouseoverDs?:undefined|((ds:string)=>void), onMouseoutDs?:undefined|((ds:string)=>void),
    orientation?: "horizontal" | "vertical",
}) {

  const sorted = [...datasources].sort((a, b) => a.localeCompare(b) + (a.startsWith("OLS.") ? 10000 : 0) + (b.startsWith("OLS.") ? -10000 : 0));

  const allSelected = dsEnabled.length === datasources.length;
  const noneSelected = dsEnabled.length === 0;

  let toggleDsEnabled=(ds:string)=>{
    if(dsEnabled.indexOf(ds) !== -1) {
      setDsEnabled(dsEnabled.filter(ds2=>ds2!==ds))
    } else {
      setDsEnabled([...dsEnabled,ds])
    }
  }

  return <div className="pt-0">
                {datasources.length > 1 && (
                  <div style={{ display: "flex", gap: "4px", marginBottom: "4px" }}>
                    <button
                      onClick={() => setDsEnabled([...datasources])}
                      disabled={allSelected}
                      style={{
                        fontSize: "11px", padding: "1px 6px", borderRadius: "4px",
                        border: "1px solid #ccc", background: allSelected ? "#f5f5f5" : "#fff",
                        color: allSelected ? "#aaa" : "#555", cursor: allSelected ? "default" : "pointer",
                      }}
                    >All</button>
                    <button
                      onClick={() => setDsEnabled([])}
                      disabled={noneSelected}
                      style={{
                        fontSize: "11px", padding: "1px 6px", borderRadius: "4px",
                        border: "1px solid #ccc", background: noneSelected ? "#f5f5f5" : "#fff",
                        color: noneSelected ? "#aaa" : "#555", cursor: noneSelected ? "default" : "pointer",
                      }}
                    >None</button>
                  </div>
                )}
                {sorted.map((ds) => {
                  const isOls = ds.startsWith("OLS.");
                  const label = isOls ? ds.split('.')[1] : ds;
                  const className = isOls ? "link-ontology" : "link-datasource";
                  return <div key={ds} style={orientation === "horizontal" ? { display: "inline", marginRight: "4px" } : { display: "flex", alignItems: "center", marginBottom: "1px" }}>
                    { datasources.length > 1 && <Checkbox size="small" style={{padding:0}} className="grebi-color" checked={dsEnabled.indexOf(ds) !== -1} onChangeCapture={() => toggleDsEnabled(ds)} />}
                      <span
                      className={`${className} px-2 py-0.5 rounded-md text-xs text-white uppercase`}
                      style={{ cursor: "pointer" }}
                      onMouseEnter={(e) => { if (onMouseoverDs) { onMouseoverDs(ds); (e.currentTarget as HTMLElement).style.outline = "2px solid #1976d2"; (e.currentTarget as HTMLElement).style.outlineOffset = "1px"; } }}
                      onMouseLeave={(e) => { if (onMouseoutDs) { onMouseoutDs(ds); (e.currentTarget as HTMLElement).style.outline = ""; } }}
                      title={isOls ? ds.split('.')[1] : ds}>{label}</span></div>
                })}</div>
}
