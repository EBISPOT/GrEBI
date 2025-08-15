import { getPaginated } from "../app/api";
import encodeNodeId from "../encodeNodeId";
import GraphNode from "../model/GraphNode";

export interface LinksTab {
    tabId:string,
    tabName:string,
    count:number
}

export default async function getNodeLinksTabs(node:GraphNode, subgraph:string):Promise<LinksTab[]> {

    let type = node.extractType()
    let metadata_promises:any = []

    if(type?.shortName === 'Gene') {
        metadata_promises.push(getGeneLinksTabs(node, subgraph))
    }

    return await Promise.all(metadata_promises)
}

async function getGeneLinksTabs(node:GraphNode, subgraph:string) {

    let page = await (getPaginated<any>(`api/v1/subgraphs/${
        subgraph
    }/nodes/${encodeNodeId(node.getNodeId())}/incoming_edges`, {
            'size': "1",
            'grebi:type': 'biolink:chemical_gene_interaction_association'
        }));

    return {
        tabId: "chemical_gene_interactions",
        tabName: "Chemical Interactions",
        count: page.totalElements
    }
}
