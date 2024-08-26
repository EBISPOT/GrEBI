import { Link } from "react-router-dom"
import GraphNodeRef from "../../../model/GraphNodeRef"
import encodeNodeId from "../../../encodeNodeId"
import NodeTypeChip from "../../../components/NodeTypeChip"
import { DatasourceTags } from "../../../components/DatasourceTag"

export default function NodeRefLink({
    subgraph,
    nodeRef
}:{
    subgraph:string,
    nodeRef:GraphNodeRef
}) {
    let type = nodeRef.extractType()

    return <Link to={`/subgraphs/${subgraph}/nodes/${encodeNodeId(nodeRef.getNodeId())}`}>
        {nodeRef.getName()}
        {type && <NodeTypeChip type={type} />}
        {/* <br/>
        <DatasourceTags dss={nodeRef.getDatasources()} /> */}
    </Link>
}