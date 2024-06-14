import { Close, KeyboardArrowDown } from "@mui/icons-material";
import { useCallback, useEffect, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { copyToClipboard, randomString, usePrevious } from "../../app/util";
import Header from "../../components/Header";
import LoadingOverlay from "../../components/LoadingOverlay";
import { Pagination } from "../../components/Pagination";
import SearchBox from "../../components/SearchBox";
import GraphNode from "../../model/GraphNode";
import { getSearchResults } from "./searchSlice";
import React from "react";
import { get, getPaginated } from "../../app/api";
import { DatasourceTags } from "../../components/DatasourceTag";

export default function Search() {
  const [searchParams] = useSearchParams();
  const search = searchParams.get("q") || "";

  let [loadingResults, setLoadingResults] = useState<boolean>(false);
  let [results, setResults] = useState<GraphNode[]>([]);
  let [totalResults, setTotalResults] = useState<number>(0);

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
    facets && Object.keys(facets).length > 0 ? facets["type"] : {};
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

  const [ontologyFacetFiltered, setOntologyFacetFiltered] = useState<object>(
    {}
  );
  useEffect(() => {
    setOntologyFacetFiltered(datasourceFacets);
  }, [JSON.stringify(datasourceFacets)]);

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

  useEffect(() => {

    async function doSearch() {
      let res = (await getPaginated<any>('api/v1/search', {
        page: page.toString(), size: rowsPerPage.toString(), q: search,
        facet: ['grebi:datasources','grebi:type']
        /*grebi__datasource: datasourceFacetselected,
        type: typeFacetSelected,
        searchParams,*/
      })).map(r => new GraphNode(r))

      setResults(res.elements)
      setFacets(res.facetFieldsToCounts)
    }

    doSearch()
  }, [
    search,
    page,
    rowsPerPage,
    datasourceFacetselected,
    typeFacetSelected,
    searchParams,
  ]);
  useEffect(() => {
    if (prevSearch !== search) setPage(0);
  }, [search, prevSearch]);

  return (
    <div>
      <Header section="home" />
      <main className="container mx-auto px-4 h-fit my-8">
        <div className="flex flex-nowrap gap-4 mb-6">
          <SearchBox initialQuery={search} />
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-4 lg:gap-8">
          <div
            className={`fixed top-0 left-0 mb-4 z-30 lg:z-0 lg:static lg:col-span-1 bg-gradient-to-r from-neutral-light to-white rounded-lg p-8 text-neutral-black overflow-x-auto h-full lg:h-fit lg:translate-x-0 transition-transform ${
              hideFilters ? "-translate-x-full" : "translate-x-0"
            }`}
          >
            <div className="flex flex-row items-center justify-between mb-4">
              <div className="font-bold text-neutral-dark text-sm mr-2">
                {`Showing ${
                  totalResults > rowsPerPage ? rowsPerPage : totalResults
                } from a total of ${totalResults}`}
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
                        .sort((a, b) => {
                          const ac = a ? a.toString() : "";
                          const bc = b ? b.toString() : "";
                          return ac.localeCompare(bc);
                        })
                        .map((key) => {
                          if (key !== "entity" && typeFacets[key] > 0) {
                            return (
                              <label
                                key={key}
                                htmlFor={key}
                                className="block p-1 w-fit"
                              >
                                <input
                                  type="checkbox"
                                  id={key}
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
                          } else return null;
                        })
                    : null}
                </fieldset>
                <div className="font-semibold text-lg mb-2">Ontology</div>
                <div className="relative grow">
                  <input
                    id="facet-search-ontology"
                    type="text"
                    autoComplete="off"
                    placeholder="Search id..."
                    className="input-default text-sm mb-3 pl-3"
                    value={ontologyFacetQuery}
                    onChange={(event) => {
                      if (event.target.value) {
                        setOntologyFacetFiltered(
                          Object.fromEntries(
                            Object.entries(datasourceFacets).filter((key) =>
                              key
                                .toString()
                                .toLowerCase()
                                .includes(event.target.value.toLowerCase())
                            )
                          )
                        );
                        setOntologyFacetQuery(event.target.value);
                      } else {
                        setOntologyFacetFiltered(datasourceFacets);
                        setOntologyFacetQuery("");
                      }
                    }}
                  />
                  {ontologyFacetQuery ? (
                    <div className="absolute right-1.5 top-1.5 z-10">
                      <button
                        type="button"
                        onClick={() => {
                          setOntologyFacetFiltered(datasourceFacets);
                          setOntologyFacetQuery("");
                        }}
                      >
                        <Close />
                      </button>
                    </div>
                  ) : null}
                </div>
                <fieldset>
                  {ontologyFacetFiltered &&
                  Object.keys(ontologyFacetFiltered).length > 0
                    ? Object.keys(ontologyFacetFiltered)
                        .sort((a, b) => {
                          const ac = a ? a.toString() : "";
                          const bc = b ? b.toString() : "";
                          return ac.localeCompare(bc);
                        })
                        .map((key) => {
                          if (ontologyFacetFiltered[key] > 0) {
                            return (
                              <label
                                key={key}
                                htmlFor={key}
                                className="block p-1 w-fit"
                              >
                                <input
                                  type="checkbox"
                                  id={key}
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
                          } else return null;
                        })
                    : null}
                </fieldset>
              </div>
            ) : null}
          </div>
          <div className="lg:col-span-3">
            <div className="flex flex-col-reverse gap-4 lg:flex-row justify-between mb-4">
              <div className="lg:basis-3/4 lg:self-center text-2xl font-bold text-neutral-dark">
                Search results for: {search}
              </div>
              <div className="justify-between flex flex-row items-center gap-4">
                <button
                  className="lg:hidden button-secondary"
                  type="button"
                  onClick={() => {
                    setHideFilters(false);
                  }}
                >
                  Filters
                </button>
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
                <Pagination
                  page={page}
                  onPageChange={setPage}
                  dataCount={totalResults}
                  rowsPerPage={rowsPerPage}
                />
                {results.map((graphNode: GraphNode) => {
                  let nodeType = graphNode.extractType()
                  return (
                    <div className="my-5">
                      <div className="my-2 leading-loose truncate flex flex-row items-center">
                        <Link to={graphNode.getLinkUrl()}
                          className={`link-default text-xl mr-2 ${
                            graphNode.isBoldForQuery(search) ? "font-bold" : ""
                          } ${graphNode.isDeprecated() ? "line-through" : ""}`}
                        >
                          {graphNode.getName()}
                        </Link>
                      { nodeType &&
                      <span style={{textTransform:'uppercase', fontVariant:'small-caps',fontWeight:'bold',fontSize:'small',verticalAlign:'middle',marginLeft:'12px',marginRight:'12px'}}>{nodeType.long}</span>
                    }
            <DatasourceTags dss={graphNode.getDatasources()} />
                    </div>
                      <div className="my-1 leading-relaxed">
                {graphNode.getIds().map(id => <span
className="bg-grey-default rounded-sm font-mono py-1 px-1 mr-1 text-sm"
>{id.value}</span>)}
                      </div>
                      {graphNode.getDescription() && (
                      <div className="my-1 leading-relaxed">
                        {graphNode.getDescription()}
                      </div>)}
                    </div>
                  );
                })}
                <Pagination
                  page={page}
                  onPageChange={setPage}
                  dataCount={totalResults}
                  rowsPerPage={rowsPerPage}
                />
              </div>
            ) : (
              <div className="text-xl text-neutral-black font-bold">
                No results!
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
      </main>
    </div>
  );
}
