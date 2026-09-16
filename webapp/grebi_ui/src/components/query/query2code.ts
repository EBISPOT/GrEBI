import { QueryTemplate } from "../../model/QueryTemplate";
import GraphNodeRef from "../../model/GraphNodeRef";
import sharedQuery2code from "../../../../query2code.mjs";

export default function query2code(queryTemplate:QueryTemplate, graph:string, params:Record<string,any>) {
    const apiUrl = (process.env.REACT_APP_APIURL || "").replace(/\/+$/, "");
    const exampleParams: Record<string, any> = (queryTemplate.examples?.length ?? 0) > 0 ? queryTemplate.examples[0].params : {};
    const paramIds = (queryTemplate.params || []).map(p => p.param_id);
    // the user's current values where set (SourceId params hold the chosen node), else the first example's
    const values: Record<string, any> = {};
    for (const id of paramIds) {
        const v = params?.[id];
        if (v instanceof GraphNodeRef) values[id] = v.getId().value;
        else if (v !== undefined && v !== null && v !== "") values[id] = v;
        else if (exampleParams[id] !== undefined) values[id] = exampleParams[id];
    }
    return sharedQuery2code(apiUrl, graph, queryTemplate.id, paramIds, values);
}

