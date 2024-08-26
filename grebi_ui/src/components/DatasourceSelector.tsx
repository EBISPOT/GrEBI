import { Checkbox } from "@mui/material"
import React from "react";

export default function DatasourceSelector({
    datasources,
    dsEnabled, setDsEnabled
}:{
    datasources:string[], 
    dsEnabled:string[], setDsEnabled:(ds:string[])=>void
}) {
  
      datasources.sort((a, b) => a.localeCompare(b) + (a.startsWith("OLS.") ? 10000 : 0) + (b.startsWith("OLS.") ? -10000 : 0))
      
  let toggleDsEnabled=(ds:string)=>{
    if(dsEnabled.indexOf(ds) !== -1) {
      setDsEnabled(dsEnabled.filter(ds2=>ds2!==ds))
    } else {
      setDsEnabled([...dsEnabled,ds])
    }
  }

  return <div className="pt-2">
                {datasources.map((ds, i) => {
                  if (ds.startsWith("OLS.")) {
                    return <span className="mr-1">
                    { datasources.length > 1 && <Checkbox size="small" style={{padding:0}} checked={dsEnabled.indexOf(ds) !== -1} onChangeCapture={() => toggleDsEnabled(ds)} />}
                      <span
                      className="link-ontology px-2 py-0.5 rounded-md text-xs text-white uppercase"
                      title={ds.split('.')[1]}>{ds.split('.')[1]}</span></span>
                  } else {
                    return <span className="mr-1">
                    { datasources.length > 1 && <Checkbox size="small" style={{padding:0}} checked={dsEnabled.indexOf(ds) !== -1} onChangeCapture={() => toggleDsEnabled(ds)} />}
                      <span
                      className="link-datasource px-2 py-0.5 rounded-md text-xs text-white uppercase"
                      title={ds}>{ds}
                      </span>
                      </span>
                  }
                })}</div>
}
