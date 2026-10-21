import { Link } from "react-router-dom"
import encodeNodeId from "../../encodeNodeId"
import GraphNodeRef from "../../model/GraphNodeRef"
import NodeTypeChip from "../NodeTypeChip"

export default function NodeRefLink({
    graph,
    nodeRef,
    showTypeChip
}:{
    graph:string,
    nodeRef:GraphNodeRef,
    showTypeChip?:boolean|undefined
}) {
    let types = nodeRef.extractTypes()

    var linkUrl = `/graphs/${graph}/nodes/${nodeRef.getEncodedNodeId()}`;

    return <Link to={linkUrl}>
        {nodeRef.getName()}
        {showTypeChip && types.map((type) => <NodeTypeChip key={type.shortName} type={type} />)}
        {/* <br/>
        <DatasourceTags dss={nodeRef.getDatasources()} /> */}
    </Link>
}