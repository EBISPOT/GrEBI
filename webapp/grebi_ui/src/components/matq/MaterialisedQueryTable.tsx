import { useState, useEffect, Fragment } from "react";
import GraphMetadata from "../../model/GraphMetadata"
import MaterialisedQuery from "../../model/MaterialisedQuery";
import LocalDataTable from "../datatable/LocalDataTable"
import { get } from "../../app/api";
import { Box, Button, CircularProgress, Link, Stack } from "@mui/material";
import { Download, Info } from "@mui/icons-material";
import { useNavigate } from "react-router-dom";


const cols= [
    {
        id:"id",
        name:"Query ID",
        selector:(row:any,key:string)=> {
            return <Fragment>
                <code>{row[key]}</code> <Link className="link-default" target="_blank" href={`https://github.com/EBISPOT/GrEBI/blob/dev/query_templates/${row[key]}.yaml`}>
<span style={{ display: 'inline-flex', alignItems: 'center', justifyContent: 'center' }}><Info style={{ fontSize: '1em' }} /></span>
                </Link>
                </Fragment>
        },
        sortable:true
    },
    {
        id:"description",
        name:"Description",
        selector:(row:any,key:string)=>row[key],
        sortable:true
    },
    {
        id:"updated",
        name:"Updated",
        selector:(row:any,key:string)=>row["end_time"],
        sortable:true
    },
    {
        id:"download",
        name:"",
        selector:(row:any,key:string)=> <Link target="_blank" href="https://ftp.ebi.ac.uk/pub/databases/spot/kg/"><Button><Box
  display="flex"
  alignItems="center"
>
                  <Download /> CSV
                </Box></Button></Link>,
        sortable:false
    }
];


export default function MaterialisedQueryTable({
    graph
}:{
    graph?:string|undefined
}) {


  let [matQs, setMatQs] = useState<MaterialisedQuery[]|null>(null);
  let [graphMetadata, setGraphMetadata] = useState<any|null>(null);
  const navigate = useNavigate();

    useEffect(() => {
        get<MaterialisedQuery[]>(`api/v1/graphs/${graph}/materialised_queries`).then(r => setMatQs(r));
    }, [graph]);

    useEffect(() => {
        if(graph)
            get<GraphMetadata>(`api/v1/graphs/${graph}`).then(r => setGraphMetadata(r));
    }, [graph]);

    if(!matQs) {
        return <CircularProgress />
    }

    if(graph && !graphMetadata) {
        return <CircularProgress />
    }

    return <LocalDataTable
                    data={matQs} 
                    addColumnsFromData={false}
                    defaultSelector={(row,key)=>row[key]}
                    columns={cols}
                    onSelectRow={(row) => {
                        const rowGraph = row["graph"] || graph;
                        if (!rowGraph) {
                            return;
                        }
                        navigate(`/graphs/${rowGraph}/tables/${row["id"]}`)
                    }}
                    />

}
