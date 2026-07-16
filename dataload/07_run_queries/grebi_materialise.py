#!/usr/bin/env python3
"""
Derivation of *materialise* Cypher queries from GrEBI query templates.

A query template opts into materialisation with a top-level ``materialise:``
block (see docs/materialise-query-templates.md). There are three kinds:

1. **Live parameterised** (no ``materialise`` block) — served live, never
   precomputed. Skipped here.

2. **Standalone materialised** (``materialise.cypher`` present, no ``params``) —
   the body *is* the materialise query (this is the old ``materialised_queries/``
   concept). Run verbatim.

3. **Materialised parameterised** (``params`` + per-param ``materialise.params``
   directives) — the template's own body doubles as the materialise query with
   each parameter's Id anchor rewritten so the base node ranges over its whole
   domain (a "closure root" substitution). No hand-written second query.

The functions here are pure string transforms so they can be unit-tested without
Neo4j (see test_grebi_materialise.py).
"""

import re


DEFAULT_BUDGET_ROWS = 15_000_000


def is_materialised(template):
    return isinstance(template, dict) and template.get("materialise") is not None


def is_standalone(template):
    m = template.get("materialise") or {}
    params = template.get("params")
    return is_materialised(template) and (not params) and m.get("cypher")


def is_parameterised(template):
    params = template.get("params")
    return is_materialised(template) and bool(params)


def materialise_mode(template):
    m = template.get("materialise") or {}
    mode = m.get("mode")
    return mode if mode else "full"


def budget_rows(template):
    m = template.get("materialise") or {}
    b = m.get("budget_rows")
    return int(b) if b else DEFAULT_BUDGET_ROWS


def runs_for_subgraph(template, subgraph):
    """Whether this materialised template should run for the given subgraph.

    Honours (in order): materialise.run_for_subgraphs (explicit allow-list) then
    the top-level `graphs` list. Absent both, runs for every subgraph.
    """
    m = template.get("materialise") or {}
    rfs = m.get("run_for_subgraphs")
    if rfs:
        return subgraph in rfs
    graphs = template.get("graphs")
    if graphs:
        return subgraph in graphs
    return True


def _param_by_id(template, param_id):
    for p in template.get("params") or []:
        if p.get("param_id") == param_id:
            return p
    return None


def _materialise_param(template, param_id):
    m = template.get("materialise") or {}
    for mp in m.get("params") or []:
        if mp.get("param_id") == param_id:
            return mp
    return None


def _sub_param_token(text, param_id, replacement):
    """Replace the Cypher parameter $param_id (word-bounded) with `replacement`."""
    pattern = r"\$" + re.escape(param_id) + r"(?![A-Za-z0-9_])"
    return re.sub(pattern, replacement, text)


def _anchor_regex(param_id):
    """Match `-[:sourceId]->(:Id {id: $param_id})` with flexible whitespace."""
    return re.compile(
        r"-\s*\[\s*:\s*sourceId\s*\]\s*->\s*"
        r"\(\s*:\s*Id\s*\{\s*id\s*:\s*\$" + re.escape(param_id) + r"\s*\}\s*\)"
    )


def _value_literal(param):
    """A Cypher literal for a non-closure (value) parameter's default."""
    default = param.get("param_default")
    if default is None:
        raise ValueError(
            f"Parameter {param.get('param_id')} has no materialise directive and "
            f"no param_default, so the template cannot be materialised"
        )
    ptype = (param.get("param_type") or "").lower()
    if ptype in ("float", "int", "integer"):
        return str(default)
    # string / anything else -> single-quoted literal
    return "'" + str(default).replace("'", "\\'") + "'"


def derive_materialise_match(template):
    """Rewrite the template's match fragment into its materialise form.

    Each parameter's Id anchor is rewritten so the base node ranges over the
    parameter's whole `param_opts` domain, per its materialise directive:

      * domain_kind=id,  closure=descendants|ancestors:
            $param  ->  'domain_root'
        (the template is authored in closure-root form
         `(base)-[:broad_match*0..1]->(x)-[:sourceId]->(:Id {id: $param})`, so
         substituting the root Id makes base range over the whole domain)

      * domain_kind=id,  closure=exact:
            -[:sourceId]->(:Id {id: $param})
              ->  -[:`biolink:broad_match`*0..1]->(__param_dom)-[:sourceId]->(:Id {id: 'domain_root'})
        (base was anchored exactly; wrap it so it ranges over the domain while
         serving still filters it exactly)

      * domain_kind=label:
            drop the `-[:sourceId]->(:Id {id: $param})` anchor entirely; the base
            keeps its type label (e.g. `(gene:`hgnc:Gene`)`) so it ranges over the
            whole labelled domain.

    Any remaining (non-closure) parameters are replaced by their param_default
    literal so the derived query has no unbound $parameters.
    """
    match = template["cypher_match_fragment"]

    materialised_ids = set()
    for mp in (template.get("materialise") or {}).get("params") or []:
        pid = mp["param_id"]
        materialised_ids.add(pid)
        closure = (mp.get("closure") or "descendants").lower()
        domain_kind = (mp.get("domain_kind") or "id").lower()
        domain_root = mp.get("domain_root")

        if domain_kind == "label":
            match = _anchor_regex(pid).sub("", match)
        elif domain_kind == "id":
            if not domain_root:
                raise ValueError(
                    f"materialise param {pid}: domain_kind=id requires domain_root"
                )
            if closure == "exact":
                repl = (
                    "-[:`biolink:broad_match`*0..1]->(__" + pid + "_dom)"
                    "-[:sourceId]->(:Id {id: '" + domain_root + "'})"
                )
                match = _anchor_regex(pid).sub(repl, match)
            else:  # descendants / ancestors -> value substitution on the root Id
                match = _sub_param_token(match, pid, "'" + domain_root + "'")
        else:
            raise ValueError(
                f"materialise param {pid}: unknown domain_kind '{domain_kind}'"
            )

    # Substitute any remaining value params (defaults) so nothing is unbound.
    for p in template.get("params") or []:
        pid = p["param_id"]
        if pid in materialised_ids:
            continue
        match = _sub_param_token(match, pid, _value_literal(p))

    return match


def _base_columns(template):
    """Result-column ids that a materialise param filters (the base nodes)."""
    cols = []
    for mp in (template.get("materialise") or {}).get("params") or []:
        col = mp.get("filters_column")
        if col and col not in cols:
            cols.append(col)
    return cols


def derive_materialise_query(template):
    """Full derived Cypher for a parameterised materialised template.

    mode=full        -> substituted match + the template's DISTINCT return.
    mode=counts_only -> substituted match + a per-base-node row-count histogram
                        (RETURN <base_col>, count(*) AS _count). Requires exactly
                        one base column.
    """
    match = derive_materialise_match(template).rstrip()
    ret = template["cypher_return_fragment"].strip()

    if materialise_mode(template) == "counts_only":
        bases = _base_columns(template)
        if len(bases) != 1:
            raise ValueError(
                "counts_only requires exactly one materialise param (base column); "
                f"found {bases}"
            )
        base_col = bases[0]
        # Turn the DISTINCT projection into an intermediate WITH, then group by the
        # base column and count the distinct result rows it contributes.
        with_frag = re.sub(r"^\s*RETURN\s+DISTINCT\b", "WITH DISTINCT", ret, count=1)
        if with_frag == ret:
            raise ValueError(
                "counts_only expects the return fragment to start with "
                "'RETURN DISTINCT'"
            )
        return (
            match + "\n" + with_frag
            + "\nRETURN `" + base_col + "`, count(*) AS _count"
        )

    return match + "\n" + ret


def standalone_query(template):
    return (template.get("materialise") or {}).get("cypher")


def query_to_run(template):
    """The Cypher to execute at dataload for a materialised template."""
    if is_standalone(template):
        return standalone_query(template)
    if is_parameterised(template):
        return derive_materialise_query(template)
    raise ValueError("template is not materialised")


def serving_metadata(template):
    """Per-template serving directives recorded in graph_metadata so the API can
    route /query/{id} to the Postgres closure path.
    """
    if is_standalone(template):
        return {"kind": "standalone"}

    params_meta = []
    for mp in (template.get("materialise") or {}).get("params") or []:
        pid = mp["param_id"]
        p = _param_by_id(template, pid) or {}
        params_meta.append({
            "param_id": pid,
            "filters_column": mp.get("filters_column"),
            "closure": (mp.get("closure") or "descendants").lower(),
            "param_type": p.get("param_type"),
        })
    return {
        "kind": "parameterised",
        "mode": materialise_mode(template),
        "params": params_meta,
    }
