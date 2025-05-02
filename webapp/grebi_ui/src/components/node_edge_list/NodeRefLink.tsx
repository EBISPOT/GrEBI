import { Link } from "react-router-dom"
import encodeNodeId from "../../encodeNodeId"
import GraphNodeRef from "../../model/GraphNodeRef"
import NodeTypeChip from "../NodeTypeChip"

export default function NodeRefLink({
    subgraph,
    nodeRef,
    showTypeChip
}:{
    subgraph:string,
    nodeRef:GraphNodeRef,
    showTypeChip?:boolean|undefined
}) {
    let type = nodeRef.extractType()

    var linkUrl = process.env.GREBI_FRONTEND === 'exposomekg' ?
     `/nodes/${nodeRef.getEncodedNodeId()}`
     :  `/subgraphs/${subgraph}/nodes/${nodeRef.getEncodedNodeId()}`;

    return <Link to={linkUrl}>
        {nodeRef.getName()}
        {showTypeChip && type && <NodeTypeChip type={type} />}
        {/* <br/>
        <DatasourceTags dss={nodeRef.getDatasources()} /> */}
    </Link>
}