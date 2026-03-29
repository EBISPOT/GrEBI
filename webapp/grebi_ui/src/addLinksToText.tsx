
import hardcodedNodeTypes from "./hardcoded_node_types.json";
import * as Muicon from "@mui/icons-material";

export default function addLinksToText(text:string, graph:string|undefined) {
    if(!graph)
        return text;

    let curieRegex = /\b([a-z]+:[a-z0-9_]+)\b/gi;

    return text.split(curieRegex).map((part, index) => {
        if(index % 2 === 0) {
            return part; // Non-CURIE part
        } else {
            // CURIE part
            let curie = part;
            for(let nodeType of hardcodedNodeTypes) {
                if(nodeType.types.indexOf(curie) !== -1) {
                    return <span
                className={`px-2 py-0.5 rounded-md text-xs uppercase font-bold ml-1`} style={{backgroundColor:nodeType.bgColor}} title={nodeType.longName}>

{ nodeType.icon &&
                    <DynamicIcon iconName={nodeType.icon} /> }
                    
                    {nodeType.longName}</span>
                }
            }
            return curie;
        }
    });
}

function DynamicIcon({ iconName }: { iconName: string }) {
    const IconComponent = Muicon[iconName as keyof typeof Muicon];
    return IconComponent ? <IconComponent fontSize="small" /> : null;
}