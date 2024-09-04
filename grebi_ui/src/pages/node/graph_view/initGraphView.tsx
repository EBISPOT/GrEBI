
import GraphNodeRef from "../../../model/GraphNodeRef";
import GraphViewCtx from "./GraphViewCtx";

export default async function initGraphView(
    container:HTMLDivElement,
    subgraph:string,
    node:GraphNodeRef
) {
    let ctx = new GraphViewCtx(container, subgraph)
    await ctx.reload(node)
}
