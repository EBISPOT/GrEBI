import { ThemeProvider, Select, MenuItem, FormControl, InputLabel } from "@mui/material";
import { Fragment, useCallback, useEffect, useRef, useState } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { get, getPaginated } from "../app/api";
import { theme } from "../app/mui";
import { joinSearchParams, randomString } from "../app/util";
import React from "react";
import GraphNode from "../model/GraphNode";
import encodeNodeId from "../encodeNodeId";
import { DatasourceTags } from "./DatasourceTag";
import NodeTypeChip from "./NodeTypeChip";
import { join } from "path";

let curSearchToken: any = null;

interface SearchBoxEntry {
  linkUrl: string;
  li: JSX.Element;
}


export default function SearchBox({
  subgraph,
  initialQuery,
  placeholder,
  collectionId,
  showExact,
  showSuggestions,
  additionalParams,
  availableModels: controlledModels,
  selectedModel: controlledSelectedModel,
  onModelChange: controlledOnModelChange,
}: {
  subgraph:string,
  initialQuery?: string;
  placeholder?: string;
  collectionId?: string;
  showExact?: boolean;
  showSuggestions?: boolean;
  additionalParams?:URLSearchParams;
  availableModels?: {model: string, can_embed: boolean}[];
  selectedModel?: string;
  onModelChange?: (model: string) => void;
}) {
  const [searchParams, setSearchParams] = useSearchParams();
  //   let lang = searchParams.get("lang") || "en";
  const navigate = useNavigate();

  const [autocomplete, setAutocomplete] = useState<string[] | null>(null);
  const [jumpTo, setJumpTo] = useState<any[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [query, setQuery] = useState<string>(initialQuery || "");
  const [isFocused, setIsFocused] = useState(false);
  const [arrowKeySelectedN, setArrowKeySelectedN] = useState<
    number | undefined
  >(undefined);

  const [internalModels, setInternalModels] = useState<{model: string, can_embed: boolean}[]>([]);
  const [internalSelectedModel, setInternalSelectedModel] = useState<string>(searchParams.get("model") || "");

  const isControlled = controlledModels !== undefined;
  const availableModels = isControlled ? controlledModels : internalModels;
  const selectedModel = isControlled ? (controlledSelectedModel || "") : internalSelectedModel;
  const handleModelChange = useCallback(
    (model: string) => {
      if (isControlled && controlledOnModelChange) {
        controlledOnModelChange(model);
      } else {
        setInternalSelectedModel(model);
      }
    },
    [isControlled, controlledOnModelChange]
  );

  useEffect(() => {
    if (isControlled) return;
    async function fetchModels() {
      try {
        const models = await get<{model: string, can_embed: boolean}[]>(`api/v1/subgraphs/${subgraph}/embedding_models`);
        setInternalModels(models || []);
        if (!searchParams.get("model") && models && models.length > 0) {
          const embeddable = models.filter(m => m.can_embed).sort((a, b) => a.model.localeCompare(b.model));
          if (embeddable.length > 0) {
            setInternalSelectedModel(embeddable[0].model);
          } else {
            setInternalSelectedModel("lexical");
          }
        } else if (!searchParams.get("model")) {
          setInternalSelectedModel("lexical");
        }
      } catch (e) {
        setInternalModels([]);
        if (!searchParams.get("model")) {
          setInternalSelectedModel("lexical");
        }
      }
    }
    fetchModels();
  }, [subgraph, isControlled]);

  const isEmbeddingSearch = selectedModel && selectedModel !== "lexical";

  let exact = searchParams.get("exactMatch") === "true";
  let obsolete = searchParams.get("includeObsoleteEntries") === "true";
  let canonical = searchParams.get("isDefiningcollection") === "true";

  const setExact = useCallback(
    (exact: boolean) => {
      let newSearchParams = new URLSearchParams(searchParams);
      newSearchParams.set("q", query);
      if (exact.toString() === "true") {
        newSearchParams.set("exactMatch", exact.toString());
      } else {
        newSearchParams.delete("exactMatch");
      }
      setSearchParams(newSearchParams);
    },
    [searchParams, setSearchParams]
  );

  if(showSuggestions === undefined) {
    showSuggestions = true;
  }

  const mounted = useRef(false);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  });

  const cancelPromisesRef = useRef(false);
  useEffect(() => {
    async function loadSuggestions() {
      if (!query) {
        setJumpTo([]);
        setAutocomplete([]);
        setLoading(false);
        return;
      }
      setLoading(true);
      setArrowKeySelectedN(undefined);

      const searchToken = randomString();
      curSearchToken = searchToken;

      const [nodes, autocomplete] = await Promise.all([
        isEmbeddingSearch
          ? get<any[]>(
              `api/v1/subgraphs/${subgraph}/semantic_search?${new URLSearchParams({
                q: query,
                model: selectedModel,
                n: "5",
              })}`
            ).then(results => results ? {
              elements: results.map((r: any) => ({
                "grebi:nodeId": r.nodeId,
                "grebi:name": r.name ? [r.name] : [],
                "grebi:datasources": r.datasources || [],
                "grebi:type": r.type || [],
                "grebi:sourceIds": r.sourceIds || [],
              }))
            } : null)
          : getPaginated<any>(
          `api/v1/subgraphs/${subgraph}/search?${joinSearchParams(new URLSearchParams({
            q: query,
            size: "5",
            lang: "en",
            exactMatch: exact.toString(),
            resolve: "false",
            includeObsoleteEntries: obsolete.toString(),
            ...(collectionId ? { collectionId } : {}),
            ...((canonical ? { isDefiningcollection: true } : {}) as any),
          }), additionalParams)}`
        ),
        showSuggestions && !isEmbeddingSearch
          ? get<string[]>(
              `api/v1/subgraphs/${subgraph}/suggest?${joinSearchParams(new URLSearchParams({
                q: query,
                exactMatch: exact.toString(),
                includeObsoleteEntries: obsolete.toString(),
              }), additionalParams)}`
            )
          : null
      ]);
      if (cancelPromisesRef.current && !mounted.current) return;

      if (searchToken === curSearchToken) {
        setJumpTo(
          nodes ? nodes.elements.map(node => new GraphNode(node)) : []
        );
        setAutocomplete(autocomplete?.filter(ac => ac !== query) || []);
        setLoading(false);
      }
    }

    loadSuggestions();

    return () => {
      cancelPromisesRef.current = true;
    };
  }, [query, exact, obsolete, canonical, isEmbeddingSearch, selectedModel]);

  let autocompleteToShow = autocomplete?.slice(0, 5) || [];
  let autocompleteElements = autocompleteToShow.map(
    (text, i): SearchBoxEntry => {
      searchParams.set("q", text);
      if (collectionId) searchParams.set("collection", collectionId);

      var linkUrl = `/graphs/${subgraph}/search?${new URLSearchParams(searchParams)}`;

      return {
        linkUrl,
        li: (
          <li
            key={text}
            className={
              "py-1 px-3 leading-7 hover:bg-link-light hover:cursor-pointer" +
              (arrowKeySelectedN === i ? " bg-link-light" : "")
            }
            onClick={() => {
              setQuery(text);
            }}
          >
            {text}
          </li>
        ),
      };
    }
  );

  let jumpToEntityElements = jumpTo
    .map((entry: GraphNode, i: number): SearchBoxEntry => {
          let name = entry.getName();
          let type = entry.extractType()
          return {
            linkUrl: entry.getLinkUrl(subgraph),
            li: (
              <li
                key={randomString()}
                className={
                  "py-1 px-3 leading-7 hover:bg-link-light hover:cursor-pointer" +
                  (arrowKeySelectedN === i + autocompleteElements.length
                    ? " bg-link-light"
                    : "")
                }
              >
                <Link
                  onClick={() => {
                    setQuery("");
                  }}
                  to={entry.getLinkUrl(subgraph)}
                >
                  <div className="flex justify-between">
                 
                    <div className="truncate flex-auto" title={name}>
                      {name}
                      {type && <NodeTypeChip type={type} />}
                    </div>
                    
                    <div className="truncate flex-initial ml-2 text-right">
                      <DatasourceTags dss={entry.getDatasources()} />
                      {/* <span
                        className="bg-orange-default px-3 py-1 rounded-lg text-sm text-white uppercase"
                        title={entry.getId().value}
                      >
                        {entry.getId().value}
                      </span>
                      { entry.getIds().length > 1 && <span><small><i> + {entry.getIds().length - 1}</i></small></span> } */}
                    </div>
                  
                  </div>
                </Link>
              </li>
            ),
          };
        })


  let allDropdownElements = [
    ...autocompleteElements,
    ...jumpToEntityElements
  ];

  return (
    <Fragment>
      <div className="w-full self-center">
        <div className="flex space-x-4 items-center mb-2">
          <div className="relative grow">
            <input
              id="home-search"
              type="text"
              autoComplete="off"
              placeholder={placeholder || "Search for knowledge about..."}
              className={`input-default text-lg pl-3 ${
                query !== "" && isFocused
                  ? "rounded-b-sm rounded-b-sm shadow-input"
                  : ""
              }`}
              onBlur={() => {
                setTimeout(function () {
                  if (mounted.current) setIsFocused(false);
                }, 500);
              }}
              onFocus={() => {
                setIsFocused(true);
              }}
              value={query}
              onChange={(e) => {
                setQuery(e.target.value);
              }}
              onKeyDown={(ev) => {
                if (ev.key === "Enter") {
                  if (
                    arrowKeySelectedN !== undefined &&
                    arrowKeySelectedN < allDropdownElements.length
                  ) {
                    navigate(allDropdownElements[arrowKeySelectedN].linkUrl);
                  } else if (query) {
                    searchParams.set("q", query);
                    if (collectionId) searchParams.set("collection", collectionId);
                    if (isEmbeddingSearch) {
                      searchParams.set("model", selectedModel);
                    } else {
                      searchParams.delete("model");
                    }

                    var linkUrl = `/graphs/${subgraph}/search?${new URLSearchParams(searchParams)}`;
                    navigate(linkUrl);
                  }
                } else if (ev.key === "ArrowDown") {
                  setArrowKeySelectedN(
                    arrowKeySelectedN !== undefined
                      ? Math.min(
                          arrowKeySelectedN + 1,
                          allDropdownElements.length
                        )
                      : 0
                  );
                } else if (ev.key === "ArrowUp") {
                  if (arrowKeySelectedN !== undefined)
                    setArrowKeySelectedN(Math.max(arrowKeySelectedN - 1, 0));
                }
              }}
            />
            <div
              className={
                loading
                  ? "spinner-default w-7 h-7 absolute right-3 top-2.5 z-10"
                  : "hidden"
              }
            />
            <ul
              className={
                query !== "" && isFocused
                  ? "list-none bg-white text-neutral-dark border-2 border-neutral-dark shadow-input rounded-b-md w-full absolute left-0 top-12 z-10"
                  : "hidden"
              }
            >
              {autocompleteElements.map((entry) => entry.li)}
              <hr />
              {jumpToEntityElements.length >
                0 && (
                <div className="pt-1 px-3 leading-7">
                  <b>Nodes</b>
                </div>
              )}
              {jumpToEntityElements.map((entry) => entry.li)}
              <hr />
              {query && (
                <div
                  className={
                    "py-1 px-3 leading-7 hover:bg-link-light hover:rounded-b-sm hover:cursor-pointer" +
                    (arrowKeySelectedN === allDropdownElements.length
                      ? " bg-link-light"
                      : "")
                  }
                  onClick={() => {
                    if (query) {
                      let params = additionalParams ? joinSearchParams(searchParams, additionalParams) : searchParams;
                      params.set("q", query);
                      if (collectionId)
                        params.set("collection", collectionId);
                      if (isEmbeddingSearch) {
                        params.set("model", selectedModel);
                      } else {
                        params.delete("model");
                      }

                      var linkUrl = `/graphs/${subgraph}/search?${new URLSearchParams(params)}`;

                      navigate(linkUrl);
                    }
                  }}
                >
                  <b className="pr-1">Search for</b>
                  {query}
                </div>
              )}
            </ul>
          </div>
          <div>
            <button
              className="button-primary text-lg font-bold self-center"
              onClick={() => {
                if (query) {
                  let params = additionalParams ? joinSearchParams(searchParams, additionalParams) : searchParams;
                  params.set("q", query);
                  if (collectionId) params.set("collection", collectionId);
                  if (isEmbeddingSearch) {
                    params.set("model", selectedModel);
                  } else {
                    params.delete("model");
                  }

                  var linkUrl = `/graphs/${subgraph}/search?${new URLSearchParams(params)}`;

                  navigate(linkUrl);
                }
              }}
            >
              Search
            </button>
          </div>
          {availableModels.filter(m => m.can_embed).length > 0 && (
            <ThemeProvider theme={theme}>
              <FormControl sx={{ minWidth: 200 }} size="small">
                <InputLabel id="model-select-label">Search Model</InputLabel>
                <Select
                  labelId="model-select-label"
                  id="model-select"
                  value={selectedModel}
                  label="Search Model"
                  onChange={(e) => handleModelChange(e.target.value)}
                >
                  <MenuItem key="lexical" value="lexical">Lexical</MenuItem>
                  {availableModels.filter(m => m.can_embed).map((model) => (
                    <MenuItem key={model.model} value={model.model}>{model.model}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </ThemeProvider>
          )}
        </div>

      </div>
    </Fragment>
  );
}
