

import ReactDOM, { render } from "react-dom"
import { get, getPaginated } from "../../app/api";
import GraphEdge from "../../model/GraphEdge";
import GraphNodeRef from "../../model/GraphNodeRef";
import LoadingOverlay from "../LoadingOverlay";
import CyWrapper from "./CyWrapper";
import GraphViewControls from "./GraphViewControls";

let formatter = Intl.NumberFormat('en', { notation: 'compact' });

let MIN_COUNT_NODE_SIZE = 30
let MAX_COUNT_NODE_SIZE = 120
let MAX_CLICKABLE_COUNT = 500

export default class GraphViewCtx {

    public dsSelectorDiv:HTMLDivElement
    public graphDiv:HTMLDivElement
    public cy:CyWrapper|null = null
    public subgraph:string

    public loadingDepth:number = 0
    public loadingOverlay:HTMLDivElement

    public allDatasources:Set<string> = new Set()
    public dsExclude:Set<string> = new Set()

    public incoming_nodeIdToEdgeCountByTypeAndDs:Map<string,any> = new Map()
    public incoming_nodeIdToEdgeIds:Map<string,Set<string>> = new Map()
    public incoming_expandedEdgeIds:Set<string> = new Set()

    public outgoing_nodeIdToEdgeCountByTypeAndDs:Map<string,any> = new Map()
    public outgoing_nodeIdToEdgeIds:Map<string,Set<string>> = new Map()
    public outgoing_expandedEdgeIds:Set<string> = new Set()

    public nodes:Map<string, GraphNodeRef> = new Map()
    public edges:Map<string, GraphEdge> = new Map()

    public root:GraphNodeRef|undefined

    getTotalIncomingEdgeCountByType(nodeId:string):Map<string,{datasources:string[], count:number, dsToCount:any}> {
        return this.getTotalEdgeCountByType(this.incoming_nodeIdToEdgeCountByTypeAndDs.get(nodeId));
    }
    getTotalOutgoingEdgeCountByType(nodeId:string):Map<string,{datasources:string[], count:number, dsToCount:any}> {
        return this.getTotalEdgeCountByType(this.outgoing_nodeIdToEdgeCountByTypeAndDs.get(nodeId));
    }
    getTotalEdgeCountByType(edgeCountByTypeAndDs:any):Map<string,{datasources:string[], count:number, dsToCount:any}> {
        let res = new Map<string, {datasources:string[],count:number, dsToCount:any}>()
        for(let type of Object.keys(edgeCountByTypeAndDs)) {
            let edgeCountByDs = edgeCountByTypeAndDs[type]
            let count = 0
            let dss = Object.keys(edgeCountByDs)
            for(let ds of dss) {
                if(this.dsExclude.has(ds))
                    continue
                count += edgeCountByDs[ds];
            }
            res.set(type, {datasources:dss,count,dsToCount:edgeCountByDs})
        }
        return res
    }

    constructor(
        container:HTMLDivElement,
        subgraph:string
    ) {
        this.subgraph = subgraph

        container.innerHTML = ''
        this.dsSelectorDiv = document.createElement('div')
        this.graphDiv = document.createElement('div')
        this.graphDiv.style.height = '500px'
        container.appendChild(this.dsSelectorDiv)
        container.appendChild(this.graphDiv)

        this.loadingOverlay = document.createElement('div')
        ReactDOM.render(<LoadingOverlay />, this.loadingOverlay)
    }

    renderToCytoscapeJson():{elements:any,style:any,layout:any} {

        let elements = [
            ...Array.from(this.nodes.values()).map((node:GraphNodeRef) => ({
                classes: 'node' + (node.getNodeId() === this.root!.getNodeId() ? ' root' : ''),
                data: {
                    id: node.getNodeId(),
                    label: node.getName()
                }
            })),
            ...Array.from(this.edges.values()).map((edge:GraphEdge) => ({
                classes: 'edge',
                data: {
                    id: edge.getEdgeId(),
                    source: edge.getFrom().getNodeId(),
                    target: edge.getTo().getNodeId(),
                }
            }))
        ];

        let constraints:any = []

        let max_count = 0
        for(let node of this.nodes.values()) {
            let nodeId = node.getNodeId()
            let outgoing_edgeCountByType = this.getTotalOutgoingEdgeCountByType(nodeId)
            let incoming_edgeCountByType = this.getTotalIncomingEdgeCountByType(nodeId)

            for(let [edgeType,{datasources,count}] of outgoing_edgeCountByType!.entries()) {
                max_count = Math.max(count, max_count)
            }
            for(let [edgeType,{datasources,count}] of incoming_edgeCountByType!.entries()) {
                max_count = Math.max(count, max_count)
            }


            for(let [edgeType,{datasources,count,dsToCount}] of outgoing_edgeCountByType!.entries()) {
                if(count === 0)
                    continue
                let countNodeId = 'count_outgoing_' + nodeId + '_' + edgeType
                elements.push({
                    classes: ['count', ...(count <= MAX_CLICKABLE_COUNT ? ['small_count'] : [])],
                    data: {
                        group: 'nodes',
                        id: countNodeId,
                        count,
                        size: (MIN_COUNT_NODE_SIZE + (count / max_count) * (MAX_COUNT_NODE_SIZE-MIN_COUNT_NODE_SIZE)),
                        datasources,
                        dsToCount,
                        action: {type:'expandEdge', nodeId:nodeId, direction:'outgoing', edgeType}
                    }
                } as any)
                elements.push({
                    classes: ['count_edge', ...(count <= MAX_CLICKABLE_COUNT ? ['small_count'] : [])],
                    data: {
                        group: 'edges',
                        id: 'to_' + countNodeId,
                        source: nodeId,
                        target: countNodeId, 
                        label: edgeType,
                        datasources,
                        action: {type:'expandEdge', nodeId:nodeId, direction:'outgoing', edgeType}
                    }
                } as any)
                if(count <= MAX_CLICKABLE_COUNT) {
                    constraints.push({ left: nodeId, right: countNodeId })
                } else {
                    constraints.push({ top: nodeId, bottom: countNodeId })
                }
            }
            for(let [edgeType,{datasources,count,dsToCount}] of incoming_edgeCountByType!.entries()) {
                if(count === 0)
                    continue
                let countNodeId = 'count_incoming_' + nodeId + '_' + edgeType
                elements.push({
                    classes: ['count', ...(count <= MAX_CLICKABLE_COUNT ? ['small_count'] : [])],
                    data: {
                        group: 'nodes',
                        id: countNodeId,
                        count,
                        size: (MIN_COUNT_NODE_SIZE + (count / max_count) * (MAX_COUNT_NODE_SIZE-MIN_COUNT_NODE_SIZE)),
                        datasources,
                        dsToCount,
                        action: {type:'expandEdge', nodeId:nodeId, direction:'incoming', edgeType}
                    },
                } as any)
                elements.push({
                    classes: ['count_edge', ...(count <= MAX_CLICKABLE_COUNT ? ['small_count'] : [])],
                    data: {
                        group: 'edges',
                        id: 'from_' + countNodeId,
                        source: countNodeId,
                        target: nodeId, 
                        label: edgeType,
                        datasources,
                        action: {type:'expandEdge', nodeId:nodeId, direction:'incoming', edgeType}
                    }
                } as any)
                if(count <= MAX_CLICKABLE_COUNT) {
                    constraints.push({ right: nodeId, left: countNodeId })
                } else {
                    constraints.push({ bottom: nodeId, top: countNodeId })
                }
            }
        }

        let style = [{
                    selector: '.root',
                    style: {
                        'background-color': '#DDD',
                        'label': 'data(label)',
                        "text-valign" : "center",
                        "text-halign": "center",
                        padding: '16px',
width: 'label',
shape:'ellipse'


                    } as any
                },
                {
                    selector: '.node',
                    style: {
                        'background-color': '#EEE',
                        'label': 'data(label)',
                        "text-valign" : "center",
"text-halign" : "center",
padding: '8px',
width: 'label',
shape:'ellipse',
                    } as any
                },
                {
                    selector: '.count',
                    style: {
                        'background-color': '#EEE',
                        'label': (node) => {
                            return formatter.format(node.data('count'))
                        },
                        "text-valign" : "center",
"text-halign" : "center",
padding: '8px',
color:'gray',
                    width: (node) => node.data('size')+'px',
                    height: (node) =>node.data('size')+'px'
                    } as any
                },
                {
                    selector: '.count_edge',
                    style: {
                        'width': 3,
                        'label': 'data(label)',
                        'line-color': '#ccc',
                        'line-dash-pattern': [6, 3],
                        'target-arrow-color': '#ccc',
                        'target-arrow-shape': 'triangle',
                        'arrow-scale': 2,
                        'curve-style': 'bezier',
                         "text-rotation": "autorotate",
                        //  'font-weight': 'bold',
                         'text-background-opacity': 1,
                         'text-background-color': 'white',
                        //  'text-border-width': 1,
                        //  'text-border-color': 'black',
                        //  'text-background-padding': '8px',
                        //  'font-size': '30px'

                    }
                },
                {
                    selector: '.small_count',
                    style: {
                        // 'background-color': '#A7C7E7',
                        // 'line-color': '#A7C7E7',
                        // 'target-arrow-color': '#A7C7E7'
                        'color':'black'
                    } as any
                },
                {
                    selector: 'node.ds_highlight',
                    style:{
                    'background-color': '#7323b7',
                    'color': 'white',
                        'label': (node) => {
                            let ds_highlight = node.data('ds_highlight')
                            return formatter.format(node.data('dsToCount')[ds_highlight])
                        },
                    }
                },
                {
                    selector: 'node.ds_onto_highlight',
                    style:{
                    'background-color': '#00827c',
                    'color': 'white',
                        'label': (node) => {
                            let ds_highlight = node.data('ds_highlight')
                            return formatter.format(node.data('dsToCount')[ds_highlight])
                        },
                    }
                },
                {
                    selector: 'edge.ds_highlight',
                    style:{
                    'line-color': '#7323b7',
                    'target-arrow-color': '#7323b7',
                    'color': '#7323b7'
                    }
                },
                {
                    selector: 'edge.ds_onto_highlight',
                    style:{
                    'line-color': '#00827c',
                    'target-arrow-color': '#00827c',
                    'color': '#00827c',
                    }
                }
            ];

        let layout = {
                name: 'fcose',
                    avoidOverlap: true,
    nodeDimensionsIncludeLabels: true,
    idealEdgeLength: edge => 500,
    numIter: 500,
    amimate: false,
    relativePlacementConstraint: constraints
            };

        return {elements,style,layout}
    }
    

    async reload(root:GraphNodeRef) {

        this.root = root

        this.allDatasources = new Set()
        this.incoming_nodeIdToEdgeCountByTypeAndDs = new Map()
        this.incoming_nodeIdToEdgeIds = new Map()
        this.outgoing_nodeIdToEdgeCountByTypeAndDs = new Map()
        this.outgoing_nodeIdToEdgeIds = new Map()
        this.nodes = new Map()
        this.edges = new Map()
        this.nodes.set(root.getNodeId(), root)

        this.startLoading()

        await this.loadAll()

        let {elements,style,layout} = this.renderToCytoscapeJson()

        console.dir(elements)

        if(this.cy)
            this.cy.destroy()

        this.cy = new CyWrapper(this.graphDiv, elements, style, layout)
        this.cy.onClickElement = (elem:any) => {
            elem.data('action') && this.doAction(elem.data('action'))
        }

        //this.dsSelectorDiv.innerHTML = ''

        let renderDsSelector = () => {
            ReactDOM.render(
                <GraphViewControls
                    datasources={Array.from(this.allDatasources)}
                    dsEnabled={Array.from(this.allDatasources).filter(el => !this.dsExclude.has(el))}
                    setDsEnabled={async (dss:string[]) => {
                        if(this.isLoading())
                            return;
                        let newExclude = new Set(this.allDatasources)
                        for(let ds of dss) {
                            newExclude.delete(ds)
                        }
                        // if(newExclude.size < this.dsExclude.size) {
                        //     this.clearHighlightedDatasource()
                        // }
                        this.dsExclude = newExclude
                        this.startLoading(false)
                        renderDsSelector()
                        await this.incrementalUpdate()
                        this.stopLoading()
                    }}
                    onMouseoverDs={(ds:string) => {
                        if(!this.dsExclude.has(ds)) {
                            this.highlightDatasource(ds)
                        }
                    }}
                    onMouseoutDs={(ds:string) => this.clearHighlightedDatasource()}
                />,
                this.dsSelectorDiv
            )
        }

        renderDsSelector()

        this.stopLoading()
    }

    highlightDatasource(ds:string) {
        let cl = ds.startsWith('OLS.') ? 'ds_onto_highlight' : 'ds_highlight'
        this.cy?.exclusiveBatch(() => {
            for(let element of this.cy!.getElements()) {
                if(element.data('datasources') && element.data('datasources').indexOf(ds) !== -1) {
                    element.data('ds_highlight', ds)
                    element.addClass(cl)
                } else {
                    element.removeClass('ds_highlight')
                    element.removeClass('ds_onto_highlight')
                }
            }
        })
    }

    clearHighlightedDatasource() {
        this.cy?.exclusiveBatch(() => {
            for(let element of this.cy!.getElements()) {
                element.data('ds_highlight', undefined)
                element.removeClass('ds_highlight')
                element.removeClass('ds_onto_highlight')
            }
        })
    }

    async incrementalUpdate() {

        let {elements,style,layout} = this.renderToCytoscapeJson()

        this.cy!.updateElements(elements)
        await this.cy!.applyLayout(layout)
    }

    startLoading(showOverlay?:boolean|undefined) {
        this.loadingDepth += 1
        if(showOverlay && this.loadingDepth === 1)
            this.graphDiv.insertBefore(this.loadingOverlay, this.graphDiv.firstChild)
    }
    stopLoading() {
        this.loadingDepth -= 1
        if(this.loadingDepth === 0 && this.loadingOverlay.parentNode)
            this.graphDiv.removeChild(this.loadingOverlay)
    }
    isLoading() {
        return this.loadingDepth > 0
    }

    async loadAll() {

        let toLoadShallow = Array.from(this.nodes.values()).filter(
                node => this.outgoing_nodeIdToEdgeCountByTypeAndDs.get(node.getNodeId()) === undefined)

        return Promise.all(toLoadShallow.map(node => this.loadShallow(node)))
    }

    async loadIncoming(node:GraphNodeRef) {

        let res = await get<any>(`api/v1/subgraphs/${this.subgraph}/nodes/${node.getEncodedNodeId()}/incoming_edge_counts`)

        let singulars =  this.getEdgeTypesWithSingularEndpoints(res)

        let resolvedSingulars = await Promise.all(singulars.map(edgeType =>
            get(`api/v1/subgraphs/${this.subgraph}/nodes/${node.getEncodedNodeId()}/incoming_edges?grebi:type=${edgeType}`)))

        for(let edgeType of singulars) {

        }

        return res
    }

    async loadOutgoing(node:GraphNodeRef) {

        let res = await get<any>(`api/v1/subgraphs/${this.subgraph}/nodes/${node.getEncodedNodeId()}/outgoing_edge_counts`)

        return res
    }

    private getEdgeTypesWithSingularEndpoints(res:any):string[] {

        // {p: datasource: count }}

        let singulars = new Set<string>()
        for(let edgeType of Object.keys(res)) {
            let dsToCount = res[edgeType]
            let dss = Object.keys(dsToCount)
            if(dss.length === 1) {
                singulars.add(edgeType)
            }
        }
        return singulars

    }


    async loadShallow(node:GraphNodeRef) {

        let [incomingEdgeCounts,outgoingEdgeCounts] = (await Promise.all([
            this.loadIncoming(node),
            this.loadOutgoing(node)
        ]))

        this.incoming_nodeIdToEdgeCountByTypeAndDs.set(node.getNodeId(), incomingEdgeCounts);
        this.outgoing_nodeIdToEdgeCountByTypeAndDs.set(node.getNodeId(), outgoingEdgeCounts);

        for(let edgeTypeToCountByDs of [incomingEdgeCounts, outgoingEdgeCounts]) {
            for(let edgeType of Object.keys(edgeTypeToCountByDs)) {
                let dsToCount = edgeTypeToCountByDs[edgeType]
                for(let ds of Object.keys(dsToCount)) {
                    //let count = dsToCount[ds]
                    this.allDatasources.add(ds)
                }
            }
        }
    }

    doAction(action:any) {

        let { type, nodeId } = action

        if(type === 'expandEdge') {

            let { direction, edgeType } = action

            if(direction === 'incoming') {
                this.incoming_expandedEdgeIds.add(edgeType)
            } else {
                this.outgoing_expandedEdgeIds.add(edgeType)
            }
        }
    }

}
