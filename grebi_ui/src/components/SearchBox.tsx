import { Checkbox, FormControlLabel, ThemeProvider } from "@mui/material";
import { Fragment, useCallback, useEffect, useRef, useState } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { get, getPaginated } from "../app/api";
import { theme } from "../app/mui";
import { randomString } from "../app/util";
import { Suggest } from "../model/Suggest";
import React, { Fragment } from "react";

let curSearchToken: any = null;

interface SearchBoxEntry {
  linkUrl: string;
  li: JSX.Element;
}

export default function SearchBox({
  initialQuery,
  placeholder,
  subgraphId,
}: {
  initialQuery?: string;
  placeholder?: string;
  subgraphId?: string;
}) {
  const [searchParams, setSearchParams] = useSearchParams();
  //   let lang = searchParams.get("lang") || "en";
  const navigate = useNavigate();

  const [autocomplete, setAutocomplete] = useState<Suggest | null>(null);
  const [jumpTo, setJumpTo] = useState<any[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [query, setQuery] = useState<string>(initialQuery || "");
  const [isFocused, setIsFocused] = useState(false);
  const [arrowKeySelectedN, setArrowKeySelectedN] = useState<
    number | undefined
  >(undefined);

  let exact = searchParams.get("exactMatch") === "true";
  let obsolete = searchParams.get("includeObsoleteEntities") === "true";
  let canonical = searchParams.get("isDefiningSubgraph") === "true";

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

  const setObsolete = useCallback(
    (obsolete: boolean) => {
      let newSearchParams = new URLSearchParams(searchParams);
      newSearchParams.set("q", query);
      if (obsolete.toString() === "true") {
        newSearchParams.set("includeObsoleteEntities", obsolete.toString());
      } else {
        newSearchParams.delete("includeObsoleteEntities");
      }
      setSearchParams(newSearchParams);
    },
    [searchParams, setSearchParams]
  );

  const setCanonical = useCallback(
    (canonical: boolean) => {
      let newSearchParams = new URLSearchParams(searchParams);
      newSearchParams.set("q", query);
      if (canonical.toString() === "true") {
        newSearchParams.set("isDefiningSubgraph", canonical.toString());
      } else {
        newSearchParams.delete("isDefiningSubgraph");
      }
      setSearchParams(newSearchParams);
    },
    [searchParams, setSearchParams]
  );

  const searchForSubgraphs = subgraphId === undefined;
  const showSuggestions = subgraphId === undefined;

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
      setLoading(true);
      setArrowKeySelectedN(undefined);

      const searchToken = randomString();
      curSearchToken = searchToken;

      const [entities, subgraphs, autocomplete] = await Promise.all([
        getPaginated<any>(
          `api/v2/entities?${new URLSearchParams({
            search: query,
            size: "5",
            lang: "en",
            exactMatch: exact.toString(),
            includeObsoleteEntities: obsolete.toString(),
            ...(subgraphId ? { subgraphId } : {}),
            ...((canonical ? { isDefiningSubgraph: true } : {}) as any),
          })}`
        ),
        searchForSubgraphs
          ? getPaginated<any>(
              `api/v2/subgraphs?${new URLSearchParams({
                search: query,
                size: "5",
                lang: "en",
                exactMatch: exact.toString(),
                includeObsoleteEntities: obsolete.toString(),
              })}`
            )
          : null,
        showSuggestions
          ? get<Suggest>(
              `api/suggest?${new URLSearchParams({
                q: query,
                exactMatch: exact.toString(),
                includeObsoleteEntities: obsolete.toString(),
              })}`
            )
          : null,
      ]);
      if (cancelPromisesRef.current && !mounted.current) return;

      if (searchToken === curSearchToken) {
        setJumpTo([
          ...entities.elements,
          ...subgraphs?.elements
        ]);
        setAutocomplete(autocomplete);
        setLoading(false);
      }
    }

    loadSuggestions();

    return () => {
      cancelPromisesRef.current = true;
    };
  }, [query, exact, obsolete, canonical]);

  let autocompleteToShow = autocomplete?.response.docs.slice(0, 5) || [];
  let autocompleteElements = autocompleteToShow.map(
    (autocomplete, i): SearchBoxEntry => {
      searchParams.set("q", autocomplete.autosuggest);
      if (subgraphId) searchParams.set("subgraph", subgraphId);
      const linkUrl = `/search?${new URLSearchParams(searchParams)}`;
      return {
        linkUrl,
        li: (
          <li
            key={autocomplete.autosuggest}
            className={
              "py-1 px-3 leading-7 hover:bg-link-light hover:cursor-pointer" +
              (arrowKeySelectedN === i ? " bg-link-light" : "")
            }
            onClick={() => {
              setQuery(autocomplete.autosuggest);
            }}
          >
            {autocomplete.autosuggest}
          </li>
        ),
      };
    }
  );

  let jumpToEntityElements = jumpTo
    .filter((thing) => thing.getType() !== "subgraph")
    .map((jumpToEntry: Thing, i: number): SearchBoxEntry => {
      const termUrl = encodeURIComponent(
        encodeURIComponent(jumpToEntry.getIri())
      );
      if (!(jumpToEntry instanceof Entity)) {
        throw new Error("jumpToEntry should be Entity");
      }
      // TODO which names to show? (multilang = lots of names)
      return jumpToEntry
        .getNames()
        .splice(0, 1)
        .map((name) => {
          const linkUrl = `/subgraphs/${jumpToEntry.getSubgraphId()}/${jumpToEntry.getTypePlural()}/${termUrl}`;
          return {
            linkUrl,
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
                  to={linkUrl}
                >
                  <div className="flex justify-between">
                    <div className="truncate flex-auto" title={name}>
                      {name}
                    </div>
                    <div className="truncate flex-initial ml-2 text-right">
                      <span
                        className="mr-2 bg-link-default px-3 py-1 rounded-lg text-sm text-white uppercase"
                        title={jumpToEntry.getSubgraphId()}
                      >
                        {jumpToEntry.getSubgraphId()}
                      </span>
                      <span
                        className="bg-orange-default px-3 py-1 rounded-lg text-sm text-white uppercase"
                        title={jumpToEntry.getShortForm()}
                      >
                        {jumpToEntry.getShortForm()}
                      </span>
                    </div>
                  </div>
                </Link>
              </li>
            ),
          };
        })[0];
    });

  let jumpToSubgraphElements = jumpTo
    .filter((thing) => thing.getType() === "subgraph")
    .map((jumpToEntry: Thing, i: number): SearchBoxEntry => {
      if (!(jumpToEntry instanceof Subgraph)) {
        throw new Error("jumpToEntry should be Subgraph");
      }
      return jumpToEntry
        .getNames()
        .splice(0, 1)
        .map(() => {
          const linkUrl = "/subgraphs/" + jumpToEntry.getSubgraphId();
          return {
            linkUrl,
            li: (
              <li
                key={jumpToEntry.getSubgraphId()}
                className={
                  "py-1 px-3 leading-7 hover:bg-link-light hover:cursor-pointer" +
                  (arrowKeySelectedN ===
                  i + jumpToEntityElements.length + autocompleteElements.length
                    ? " bg-link-light"
                    : "")
                }
              >
                <Link
                  onClick={() => {
                    setQuery("");
                  }}
                  to={linkUrl}
                >
                  <div className="flex">
                    <span
                      className="truncate text-link-dark font-bold"
                      title={
                        jumpToEntry.getName() || jumpToEntry.getSubgraphId()
                      }
                    >
                      {jumpToEntry.getName() || jumpToEntry.getSubgraphId()}
                    </span>
                  </div>
                </Link>
              </li>
            ),
          };
        })[0];
    });

  let allDropdownElements = [
    ...autocompleteElements,
    ...jumpToEntityElements,
    ...jumpToSubgraphElements,
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
                    if (subgraphId) searchParams.set("subgraph", subgraphId);
                    navigate(`/search?${new URLSearchParams(searchParams)}`);
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
              {jumpToEntityElements.length + jumpToSubgraphElements.length >
                0 && (
                <div className="pt-1 px-3 leading-7">
                  <b>Jump to</b>
                </div>
              )}
              {jumpToEntityElements.map((entry) => entry.li)}
              {jumpToSubgraphElements.map((entry) => entry.li)}
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
                      searchParams.set("q", query);
                      if (subgraphId)
                        searchParams.set("subgraph", subgraphId);
                      navigate(`/search?${new URLSearchParams(searchParams)}`);
                    }
                  }}
                >
                  <b className="pr-1">Search OLS for </b>
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
                  searchParams.set("q", query);
                  if (subgraphId) searchParams.set("subgraph", subgraphId);
                  navigate(`/search?${new URLSearchParams(searchParams)}`);
                }
              }}
            >
              Search
            </button>
          </div>
        </div>
        <div className="col-span-2">
          <ThemeProvider theme={theme}>
            <FormControlLabel
              control={
                <Checkbox
                  checked={exact}
                  onChange={(ev) => setExact(!!ev.target.checked)}
                />
              }
              label="Exact match"
            />
          </ThemeProvider>
        </div>
      </div>
    </Fragment>
  );
}
