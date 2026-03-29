
import { Close, KeyboardArrowDown } from "@mui/icons-material";
import { Fragment, useCallback, useEffect, useState } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";
import { get, getPaginated } from "../app/api";
import { usePrevious, copyToClipboard, joinSearchParams } from "../app/util";
import GraphNode from "../model/GraphNode";
import CollapsingIdList from "./CollapsingIdList";
import { DatasourceTags } from "./DatasourceTag";
import LoadingOverlay from "./LoadingOverlay";
import SearchBox from "./SearchBox";

export default function SeachInterface(opts:{ graph:string }
) {
    let { graph } = opts

  const [searchParams] = useSearchParams();
  const search = searchParams.get("q") || "";

  let [loadingResults, setLoadingResults] = useState<boolean>(true);
  let [results, setResults] = useState<GraphNode[]>([]);
  let [totalResults, setTotalResults] = useState<number>(0);
  let [searchScores, setSearchScores] = useState<Map<string, number>>(new Map());

  let [facets, setFacets] = useState<any>({});

  const prevSearch = usePrevious(search);

  const [page, setPage] = useState<number>(0);
  const [rowsPerPage, setRowsPerPage] = useState<number>(10);
  const [ontologyFacetQuery, setOntologyFacetQuery] = useState<string>("");
  const [hideFilters, setHideFilters] = useState<boolean>(true);

  const datasourceFacets =
    facets && Object.keys(facets).length > 0 ? facets["grebi:datasources"] : {};
  const [datasourceFacetselected, setDatasourceFacetselected] = useState<string[]>(
    []
  );
  const handleOntologyFacet = useCallback(
    (checked, key) => {
      let selected: string[] = datasourceFacetselected;
      if (checked) {
        selected = [...selected, key];
      } else {
        selected = selected.filter((facet) => facet !== key);
      }
      setDatasourceFacetselected((prev) => {
        if (selected !== prev) setPage(0);
        return selected;
      });
    },
    [datasourceFacetselected, setDatasourceFacetselected]
  );
  const typeFacets =
    facets && Object.keys(facets).length > 0 ? facets["grebi:type"] : {};
  const [typeFacetSelected, setTypeFacetSelected] = useState<string[]>([]);
  const handleTypeFacet = useCallback(
    (checked, key) => {
      let selected: string[] = typeFacetSelected;
      if (checked) {
        selected = [...selected, key];
      } else {
        selected = selected.filter((facet) => facet !== key);
      }
      setTypeFacetSelected((prev) => {
        if (selected !== prev) setPage(0);
        return selected;
      });
    },
    [typeFacetSelected, setTypeFacetSelected]
  );

  const ontologyFacetFiltered = ontologyFacetQuery
    ? Object.fromEntries(
        Object.entries(datasourceFacets || {}).filter(([k]) =>
          k.toLowerCase().includes(ontologyFacetQuery.toLowerCase())
        )
      )
    : datasourceFacets || {};

  const [isShortFormCopied, setIsShortFormCopied] = useState(false);
  const copyShortForm = (text: string) => {
    copyToClipboard(text)
      .then(() => {
        setIsShortFormCopied(true);
        // revert after a few seconds
        setTimeout(() => {
          setIsShortFormCopied(false);
        }, 500);
      })
      .catch((err) => {
        console.log(err);
      });
  };

  const model = searchParams.get("model");
  const isSemanticSearch = model && model !== "lexical";

  useEffect(() => {

    async function doSearch() {
      setLoadingResults(true)

      if (isSemanticSearch) {
        // Semantic search via embeddings with resolve for full data
        const semanticResults = await get<any[]>(
          `api/v1/graphs/${graph}/semantic_search?${new URLSearchParams({
            q: search,
            model: model,
            n: ((page + 1) * rowsPerPage).toString(),
            resolve: "true",
          })}`
        );

        const scores = new Map<string, number>();
        const mapped = (semanticResults || []).map(r => {
          const nodeId = r["grebi:nodeId"];
          const score = r["grebi:searchScore"];
          if (nodeId && score !== undefined) {
            scores.set(nodeId, score);
          }
          return new GraphNode(r);
        });
        setResults(mapped);
        setSearchScores(scores);
        // If we got as many as requested, there are likely more
        const requested = (page + 1) * rowsPerPage;
        setTotalResults(mapped.length >= requested ? mapped.length + 1 : mapped.length);
        setFacets({});
      } else {
        // Standard lexical search
        const filterParams: string[][] = [
          ['page', page.toString()],
          ['size', rowsPerPage.toString()],
          ['q', search],
          ['facet', 'grebi:datasources'],
          ['facet', 'grebi:type']
        ];
        for (const ds of datasourceFacetselected) {
          filterParams.push(['grebi:datasources', ds]);
        }
        for (const t of typeFacetSelected) {
          filterParams.push(['grebi:type', t]);
        }
        let res = (await getPaginated<any>(`api/v1/graphs/${graph}/search`, joinSearchParams(searchParams, new URLSearchParams(filterParams))))
        
        let mapped = res.map(r => new GraphNode(r))

        if (page === 0) {
          setResults(mapped.elements);
        } else {
          setResults(prev => [...prev, ...mapped.elements]);
        }
        setTotalResults(mapped.totalElements);
        setFacets(mapped.facetFieldsToCounts)
      }

      setLoadingResults(false)
    }

    doSearch()
  }, [
    search,
    page,
    rowsPerPage,
    datasourceFacetselected,
    typeFacetSelected,
    searchParams,
    graph,
    model
  ]);
  useEffect(() => {
    if (prevSearch !== search) setPage(0);
  }, [search, prevSearch]);

  return <Fragment>
        <div className="flex flex-nowrap gap-4 mb-6">
          <SearchBox graph={graph} initialQuery={search} />
        </div>
        <div className={`grid grid-cols-1 ${isSemanticSearch ? '' : 'lg:grid-cols-4 lg:gap-8'}`}>
          {!isSemanticSearch && <div
            className={`fixed top-0 left-0 mb-4 z-30 lg:z-0 lg:static lg:col-span-1 bg-gradient-to-r from-neutral-light to-white rounded-lg p-8 text-neutral-black overflow-x-auto h-full lg:h-fit lg:translate-x-0 transition-transform ${
              hideFilters ? "-translate-x-full" : "translate-x-0"
            }`}
          >
            <div className="flex flex-row items-center justify-between mb-4">
              <div className="font-bold text-neutral-dark text-sm mr-2">
                Filter results
              </div>
              <button
                className="lg:hidden"
                type="button"
                onClick={() => {
                  setHideFilters(true);
                }}
              >
                <Close />
              </button>
            </div>
            {totalResults > 0 ? (
              <div className="text-neutral-black">
                <div className="font-semibold text-lg mb-2">Type</div>
                <fieldset className="mb-4">
                  {typeFacets && Object.keys(typeFacets).length > 0
                    ? Object.keys(typeFacets)
                        .filter((key) => key !== "entity" && typeFacets[key] > 0)
                        .sort((a, b) => {
                          const ac = a ? a.toString() : "";
                          const bc = b ? b.toString() : "";
                          return ac.localeCompare(bc);
                        })
                        .map((key) => {
                            return (
                              <label
                                key={key}
                                htmlFor={`type-facet-${key}`}
                                className="block p-1 w-fit"
                              >
                                <input
                                  type="checkbox"
                                  id={`type-facet-${key}`}
                                  className="invisible hidden peer"
                                  checked={typeFacetSelected.includes(key)}
                                  onChange={(e) => {
                                    handleTypeFacet(e.target.checked, key);
                                  }}
                                />
                                <span className="input-checkbox mr-4" />
                                <span className="capitalize mr-4">
                                  {key} &#40;{typeFacets[key]}&#41;
                                </span>
                              </label>
                            );
                        })
                    : null}
                </fieldset>
                <div className="font-semibold text-lg mb-2">Datasource</div>
                <div className="relative grow">
                  <input
                    id="facet-search-ontology"
                    type="text"
                    autoComplete="off"
                    placeholder="Filter datasources..."
                    className="input-default text-sm mb-3 pl-3"
                    value={ontologyFacetQuery}
                    onChange={(event) => {
                      setOntologyFacetQuery(event.target.value);
                    }}
                  />
                  {ontologyFacetQuery ? (
                    <div className="absolute right-1.5 top-1.5 z-10">
                      <button
                        type="button"
                        onClick={() => {
                          setOntologyFacetQuery("");
                        }}
                      >
                        <Close />
                      </button>
                    </div>
                  ) : null}
                </div>
                <fieldset className="max-h-80 overflow-y-auto border border-neutral-300 rounded-md p-2">
                  {ontologyFacetFiltered &&
                  Object.keys(ontologyFacetFiltered).length > 0
                    ? Object.keys(ontologyFacetFiltered)
                        .filter((key) => ontologyFacetFiltered[key] > 0)
                        .sort((a, b) => {
                          return ontologyFacetFiltered[b] - ontologyFacetFiltered[a];
                        })
                        .map((key) => {
                            return (
                              <label
                                key={key}
                                htmlFor={`ds-facet-${key}`}
                                className="block p-1 w-fit"
                              >
                                <input
                                  type="checkbox"
                                  id={`ds-facet-${key}`}
                                  className="invisible hidden peer"
                                  checked={datasourceFacetselected.includes(key)}
                                  onChange={(e) => {
                                    handleOntologyFacet(e.target.checked, key);
                                    setOntologyFacetQuery("");
                                  }}
                                />
                                <span className="input-checkbox mr-4" />
                                <span className="uppercase mr-4">
                                  {key} &#40;{ontologyFacetFiltered[key]}&#41;
                                </span>
                              </label>
                            );
                        })
                    : null}
                </fieldset>
              </div>
            ) : null}
          </div>}
          <div className={isSemanticSearch ? "lg:col-span-1" : "lg:col-span-3"}>
            <div className="flex flex-col-reverse gap-4 lg:flex-row justify-between mb-4">
              <div className="lg:basis-3/4 lg:self-center text-2xl font-bold text-neutral-dark">
                Search results for: {search}
              </div>
              <div className="justify-between flex flex-row items-center gap-4">
                {!isSemanticSearch && <button
                  className="lg:hidden button-secondary"
                  type="button"
                  onClick={() => {
                    setHideFilters(false);
                  }}
                >
                  Filters
                </button>}
                <div className="flex-none flex group relative text-md">
                  <label className="self-center px-3">Show</label>
                  <select
                    className="input-default appearance-none pr-7 z-20 bg-transparent"
                    onChange={(e) => {
                      const rows = parseInt(e.target.value);
                      setRowsPerPage((prev) => {
                        if (rows !== prev) setPage(0);
                        return rows;
                      });
                    }}
                  >
                    <option value={10}>10</option>
                    <option value={25}>25</option>
                    <option value={100}>100</option>
                  </select>
                  <div className="absolute right-2 top-2 z-10 text-neutral-default group-focus:text-neutral-dark group-hover:text-neutral-dark">
                    <KeyboardArrowDown fontSize="medium" />
                  </div>
                </div>
              </div>
            </div>
            {results.length > 0 ? (
              <div>
                {results.map((graphNode: GraphNode) => {
                  let nodeType = graphNode.extractType()
                  let score = searchScores.get(graphNode.getNodeId())
                  return (
                    <div className="my-5">
                      <div className="my-2 leading-loose truncate flex flex-row items-center">
                        <Link to={graphNode.getLinkUrl(graph)}
                          className={`link-default text-xl mr-2 ${
                            graphNode.isBoldForQuery(search) ? "font-bold" : ""
                          } ${graphNode.isDeprecated() ? "line-through" : ""}`}
                        >
                          {graphNode.getName()}
                        </Link>
                        {score !== undefined && (
                          <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium mr-2"
                            style={{backgroundColor: '#f3e8ff', color: '#7c3aed'}}>
                            {(score * 100).toFixed(1)}%
                          </span>
                        )}
                      { nodeType &&
                      <span style={{textTransform:'uppercase', fontVariant:'small-caps',fontWeight:'bold',fontSize:'small',verticalAlign:'middle',marginLeft:'12px',marginRight:'12px'}}>{nodeType.longName}</span>
                    }
            <DatasourceTags dss={graphNode.getDatasources()} />
                    </div>
                    <CollapsingIdList ids={graphNode.getSourceIds()} />
                      {graphNode.getDescription() && (
                      <div className="my-1 leading-relaxed">
                        {graphNode.getDescription()}
                      </div>)}
                    </div>
                  );
                })}
                {results.length < totalResults && (
                  <div className="flex justify-center p-4">
                    <button
                      className="px-6 py-2 text-neutral-default hover:bg-neutral-default hover:rounded-md hover:text-white font-medium"
                      onClick={() => setPage(prev => prev + 1)}
                    >
                      Load more results...
                    </button>
                  </div>
                )}
              </div>
            ) : (
              <div className="text-xl text-neutral-black font-bold">
              </div>
            )}
          </div>
        </div>
        <div
          className={`fixed top-0 right-0 backdrop-blur-none h-full w-full ${
            hideFilters ? "hidden" : "z-20"
          }`}
          onClick={() => setHideFilters(true)}
        />
        {loadingResults ? (
          <LoadingOverlay message="Search results loading..." />
        ) : null}
    </Fragment>
}
