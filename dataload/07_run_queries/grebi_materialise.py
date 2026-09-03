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

3. **Materialised parameterised** (``params`` + a ``materialise`` block) — the
   template's own body doubles as the materialise query with each SourceId
   parameter's Id anchor rewritten so the base node ranges over the parameter's
   whole value space. No hand-written second query.

Each SourceId parameter declares only its value space, one flat field:

  values_under: '<curie>'      value is that node or a broad_match descendant
  values_with_type: '<label>'  value is any node carrying that type label
  (neither)                    unconstrained

Everything else is derived:

  * ``filters_column`` — the result column this parameter filters — is the
    param_id minus its ``_id`` suffix (``cell_type_id`` filters ``cell_type``),
    validated against ``result_columns``.

  * how serving matches stored rows against the queried value follows from the
    anchor's shape in the match fragment:

      (base)-[:`biolink:broad_match`*0..1]->(x)-[:sourceId]->(:Id {id: $p})
          -> rows whose base is the queried node or a descendant
      (base)-[:sourceId]->(:Id {id: $p})
          -> rows whose base is exactly the queried node

    so the body *is* the declaration; the two cannot disagree. As a guard, the
    variable returned ``AS <filters_column>`` must be the anchored variable
    itself — a template that returns some *other* roll-up variable under that
    alias (the old separate reversed-broad_match style) is rejected: rewrite it
    in the canonical closure-root (first) form instead.

The functions here are pure string transforms so they can be unit-tested without
Neo4j (see test_grebi_materialise.py).
"""

import hashlib
import re


DEFAULT_BUDGET_ROWS = 15_000_000

# Postgres truncates identifiers to 63 bytes (NAMEDATALEN - 1).
PG_MAX_IDENTIFIER = 63


def pg_identifier(name):
    """`name`, made safe against Postgres's 63-byte identifier limit.

    Postgres would otherwise truncate silently, and two long names sharing a
    prefix would collide; keep the readable head and append a short hash of the
    full name only when it would overflow.
    """
    if len(name) <= PG_MAX_IDENTIFIER:
        return name
    digest = hashlib.sha1(name.encode()).hexdigest()[:8]
    return name[:PG_MAX_IDENTIFIER - 9] + "_" + digest


def table_name(subgraph, query_id):
    """The Postgres table a materialised query's rows are stored in.

    One typed table per (subgraph, query). Computed once here at materialise
    time and recorded in the query metadata — every consumer (loader, API)
    reads the recorded name rather than re-deriving it.
    """
    return pg_identifier(f"matq_{subgraph}_{query_id}")


def is_materialised(template):
    # Only a `materialise:` *block* (mapping) opts in. `materialise: false` (or any
    # non-mapping) means live — matching the documented opt-out.
    return isinstance(template, dict) and isinstance(template.get("materialise"), dict)


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


def closure_params(template):
    """The parameters that anchor a base node (all SourceId params)."""
    return [p for p in template.get("params") or []
            if (p.get("param_type") or "").lower() == "sourceid"]


def filters_column(param):
    """The result column a SourceId parameter filters: param_id minus `_id`."""
    pid = param.get("param_id") or ""
    if not pid.endswith("_id") or len(pid) <= 3:
        raise ValueError(
            f"SourceId parameter '{pid}' must be named <result_column>_id so its "
            f"filter column can be derived"
        )
    return pid[:-3]


def _sub_param_token(text, param_id, replacement):
    """Replace the Cypher parameter $param_id (word-bounded) with `replacement`.

    Uses a function replacement so backslashes/quotes in `replacement` (e.g. an
    escaped string literal) are inserted literally, not interpreted as re escapes.
    """
    pattern = r"\$" + re.escape(param_id) + r"(?![A-Za-z0-9_])"
    return re.sub(pattern, lambda _m: replacement, text)


def _anchor_regex(param_id):
    """The parameter's Id anchor: base node, optional broad_match hop, sourceId.

    Groups: base (the base node pattern, kept by the drop/wrap transforms),
    var / label (its variable and optional type label), hop / hopvar (the
    closure-root hop, when the template is in closure-root form).
    """
    return re.compile(
        r"(?P<base>\(\s*(?P<var>[A-Za-z_][A-Za-z0-9_]*)\s*"
        r"(?::\s*(?P<label>`[^`]+`|[A-Za-z_][A-Za-z0-9_:]*))?\s*\))"
        r"(?P<hop>\s*-\s*\[\s*:\s*`biolink:broad_match`\s*\*\s*0\s*\.\.\s*1\s*\]"
        r"\s*->\s*\(\s*(?P<hopvar>[A-Za-z_][A-Za-z0-9_]*)\s*\)\s*)?"
        r"-\s*\[\s*:\s*sourceId\s*\]\s*->\s*"
        r"\(\s*:\s*Id\s*\{\s*id\s*:\s*\$" + re.escape(param_id) + r"\s*\}\s*\)"
    )


def _find_anchor(template, param):
    pid = param["param_id"]
    fragment = template["cypher_match_fragment"]
    matches = list(_anchor_regex(pid).finditer(fragment))
    if not matches:
        raise ValueError(
            f"parameter {pid}: no `-[:sourceId]->(:Id {{id: ${pid}}})` anchor found "
            f"in cypher_match_fragment; a materialised template must anchor each "
            f"SourceId parameter"
        )
    if len(matches) > 1:
        raise ValueError(
            f"parameter {pid}: anchor appears {len(matches)} times in "
            f"cypher_match_fragment; expected exactly one"
        )
    return matches[0]


def _strip_backticks(label):
    return label.strip("`") if label else None


def _strip_line_comments(cypher):
    return re.sub(r"^\s*//[^\n]*$", "", cypher or "", flags=re.MULTILINE)


def _assert_anchor_var_is_returned(template, param, anchor):
    """The variable projected `AS <filters_column>` must be the anchored one.

    In the old non-canonical style the anchor bound one variable while a
    separate reversed broad_match roll-up produced the variable actually
    returned under the filter column's alias — so the anchor's shape said
    "exact" while the rows really covered descendants. Deriving matching from
    the anchor shape would then silently serve wrong rows; reject and require
    the canonical closure-root form.
    """
    pid = param["param_id"]
    var = anchor.group("var")
    fc = filters_column(param)
    ret = _strip_line_comments(template.get("cypher_return_fragment"))
    m = re.search(
        r"([A-Za-z_][A-Za-z0-9_]*)\s*\{[^}]*\}\s+AS\s+`?" + re.escape(fc)
        + r"`?(?![A-Za-z0-9_])", ret)
    if m and m.group(1) != var:
        raise ValueError(
            f"parameter {pid}: the anchor binds ({var}) but the return fragment "
            f"projects ({m.group(1)}) AS {fc}, so the anchor's shape does not "
            f"describe the rows. Rewrite the template in the canonical "
            f"closure-root form:\n"
            f"  MATCH ({m.group(1)})-[:`biolink:broad_match`*0..1]->(root)"
            f"-[:sourceId]->(:Id {{id: ${pid}}})"
        )


def derived_matches(template, param):
    """How serving matches stored base rows against the queried value.

    descendants — the anchor is in closure-root form (broad_match hop), so the
                  stored base is the queried node or one of its descendants.
    exact       — the anchor binds the base directly, so the stored base is
                  exactly the queried node.
    """
    anchor = _find_anchor(template, param)
    _assert_anchor_var_is_returned(template, param, anchor)
    return "descendants" if anchor.group("hop") else "exact"


def _value_literal(param):
    """A Cypher literal for a non-closure (value) parameter's default."""
    default = param.get("param_default")
    if default is None:
        raise ValueError(
            f"Parameter {param.get('param_id')} has no param_default, so the "
            f"template cannot be materialised"
        )
    ptype = (param.get("param_type") or "").lower()
    if ptype in ("float", "int", "integer"):
        return str(default)
    # string / anything else -> single-quoted literal
    return "'" + str(default).replace("'", "\\'") + "'"


def derive_materialise_match(template):
    """Rewrite the template's match fragment into its materialise form.

    Each SourceId parameter's Id anchor is rewritten so the base node ranges
    over the parameter's whole value space:

      * values_under + closure-root anchor:
            $param  ->  'values_under'
        (substituting the root Id makes the base range over the whole subtree)

      * values_under + bare anchor:
            -[:sourceId]->(:Id {id: $param})
              ->  -[:`biolink:broad_match`*0..1]->(__param_dom)-[:sourceId]->(:Id {id: 'values_under'})
        (the base stays exactly-matched at serving; the wrap only frees it over
         the subtree at materialise time)

      * values_with_type (or no values_* at all):
            drop the anchor — hop included — so the base ranges over its whole
            labelled domain (values_with_type requires the base to carry the
            declared label) or, unconstrained, over whatever the rest of the
            fragment allows.

    Any remaining (non-SourceId) parameters are replaced by their param_default
    literal so the derived query has no unbound $parameters.
    """
    match = template["cypher_match_fragment"]

    closure_ids = set()
    for p in closure_params(template):
        pid = p["param_id"]
        closure_ids.add(pid)
        values_under = p.get("values_under")
        values_with_type = p.get("values_with_type")
        if values_under and values_with_type:
            raise ValueError(
                f"parameter {pid}: values_under and values_with_type are mutually "
                f"exclusive"
            )

        anchor = _anchor_regex(pid).search(match)
        if anchor is None:
            raise ValueError(
                f"parameter {pid}: no Id anchor found in cypher_match_fragment"
            )

        if values_under:
            if anchor.group("hop"):
                # closure-root form: substituting the subtree root makes the base
                # range over all its descendants (= the whole value space).
                match = _sub_param_token(match, pid, "'" + values_under + "'")
            else:
                repl = (
                    "-[:`biolink:broad_match`*0..1]->(__" + pid + "_dom)"
                    "-[:sourceId]->(:Id {id: '" + values_under + "'})"
                )
                match = _anchor_regex(pid).sub(
                    lambda m: m.group("base") + repl, match, count=1)
        else:
            if values_with_type:
                label = _strip_backticks(anchor.group("label"))
                if label != values_with_type:
                    raise ValueError(
                        f"parameter {pid}: values_with_type '{values_with_type}' "
                        f"requires the anchored base node to carry that label "
                        f"(found: {label})"
                    )
            hopvar = anchor.group("hopvar")
            if hopvar:
                residue = _strip_line_comments(
                    match[:anchor.start()] + match[anchor.end():])
                if re.search(r"\b" + re.escape(hopvar) + r"\b", residue):
                    raise ValueError(
                        f"parameter {pid}: cannot drop the anchor because its "
                        f"closure-root variable ({hopvar}) is referenced elsewhere"
                    )
            match = _anchor_regex(pid).sub(
                lambda m: m.group("base"), match, count=1)

    # Substitute any remaining value params (defaults) so nothing is unbound.
    for p in template.get("params") or []:
        pid = p["param_id"]
        if pid in closure_ids:
            continue
        match = _sub_param_token(match, pid, _value_literal(p))

    return match


def _base_columns(template):
    """Result-column ids that the closure params filter (the base nodes)."""
    cols = []
    for p in closure_params(template):
        col = filters_column(p)
        if col not in cols:
            cols.append(col)
    return cols


def _validate_materialise(template):
    """Fail fast on a mis-declared template (before we run the query)."""
    m = template.get("materialise") or {}
    if m.get("params"):
        raise ValueError(
            "materialise.params is no longer supported; declare the value space "
            "on each param instead (values_under / values_with_type)"
        )
    cps = closure_params(template)
    if not cps:
        raise ValueError(
            "a materialised parameterised template needs at least one SourceId "
            "parameter"
        )
    result_cols = {c.get("column_id") for c in template.get("result_columns") or []}
    for p in cps:
        fc = filters_column(p)
        # Real templates always declare result_columns; validate the base column
        # against them when present.
        if result_cols and fc not in result_cols:
            raise ValueError(
                f"parameter '{p.get('param_id')}': derived filter column '{fc}' is "
                f"not a result column (columns: {sorted(result_cols)})"
            )
        # Derivation side effects: anchor presence/uniqueness + canonical-form
        # guard for bare anchors.
        derived_matches(template, p)


def _assert_no_unbound_params(template, query):
    """Guard against a derivation that left a template parameter unsubstituted."""
    for p in template.get("params") or []:
        pid = p.get("param_id")
        if re.search(r"\$" + re.escape(pid) + r"(?![A-Za-z0-9_])", query):
            raise ValueError(
                f"derived materialise query still references unbound parameter ${pid}"
            )


# ---------------------------------------------------------------------------
# TRANSITIONAL: ontology datasource naming. Mirrors
# GrebiQueryTemplatesRepo.normaliseOntologyDatasourceNames on the Java side —
# keep the two in step until every deployed release uses one naming scheme.
#
# The OLS-JSON ingest names per-ontology datasources "OLS.<ont>"; the newer
# owlmake/ubergraph ingest derives "Ontologies.<ont>" from rdfs:isDefinedBy.
# Templates are written against Ontologies.*, deployed graphs serve OLS.*.
#
# The API rewrote this at template load time, but the dataload reads the YAML
# directly and so kept the raw literal — which matches nothing on a deployed
# graph. That is why disease_to_phenotypes materialised 0 rows while serving
# fine live: the two paths disagreed about what the template meant.
# ---------------------------------------------------------------------------

_DATASOURCE_PREDICATE = re.compile(
    r"""(['"])(?:Ontologies|OLS)\.([A-Za-z0-9_]+)\1\s+IN\s+"""
    r"""([A-Za-z_][A-Za-z0-9_]*\.`grebi:datasources`)"""
)

_ANY_ONTOLOGY_DATASOURCE_LITERAL = re.compile(
    r"""(['"])(?:Ontologies|OLS)\.[A-Za-z0-9_]+\1"""
)


def normalise_ontology_datasource_names(cypher):
    """Accept either naming for `"<Ontologies|OLS>.<ont>" IN <var>.`grebi:datasources``."""
    if not cypher or ("Ontologies." not in cypher and "OLS." not in cypher):
        return cypher

    def _repl(m):
        ontology, datasources_expr = m.group(2), m.group(3)
        return (f'any(__grebi_ds IN {datasources_expr} WHERE __grebi_ds IN '
                f'["Ontologies.{ontology}", "OLS.{ontology}"])')

    return _DATASOURCE_PREDICATE.sub(_repl, cypher)


def warn_on_unrecognised_datasource_literals(template_id, cypher):
    """Be loud about literals in a shape the rewrite cannot reach.

    Scans the ORIGINAL fragment with the handled predicates stripped — scanning
    the rewritten text would match the literals the rewrite itself emits.
    """
    if not cypher:
        return
    residue = _DATASOURCE_PREDICATE.sub("", cypher)
    for m in _ANY_ONTOLOGY_DATASOURCE_LITERAL.finditer(residue):
        print(f"WARNING: query template '{template_id}' contains ontology datasource "
              f"literal {m.group()} in a shape the Ontologies./OLS. normaliser does not "
              f"recognise; it will only match one naming scheme and may silently "
              f"materialise no rows")


def derive_materialise_query(template):
    """Full derived Cypher for a parameterised materialised template.

    mode=full        -> substituted match + the template's DISTINCT return.
    mode=counts_only -> substituted match + a per-base-node row-count histogram
                        (RETURN <base_col>, count(*) AS _count). Requires exactly
                        one base column.
    """
    _validate_materialise(template)

    match = derive_materialise_match(template).rstrip()
    ret = template["cypher_return_fragment"].strip()

    if materialise_mode(template) == "counts_only":
        bases = _base_columns(template)
        if len(bases) != 1:
            raise ValueError(
                "counts_only requires exactly one SourceId parameter (base "
                f"column); found {bases}"
            )
        base_col = bases[0]
        # Turn the DISTINCT projection into an intermediate WITH, then group by the
        # base column (a RETURN alias) and count the distinct result rows.
        with_frag = re.sub(r"^\s*RETURN\s+DISTINCT\b", "WITH DISTINCT", ret, count=1)
        if with_frag == ret:
            raise ValueError(
                "counts_only expects the return fragment to start with "
                "'RETURN DISTINCT'"
            )
        query = (
            match + "\n" + with_frag
            + "\nRETURN `" + base_col + "`, count(*) AS _count"
        )
    else:
        query = match + "\n" + ret

    _assert_no_unbound_params(template, query)

    tid = template.get("id") or "<unknown>"
    for frag in ("cypher_match_fragment", "cypher_return_fragment", "cypher_count_fragment"):
        warn_on_unrecognised_datasource_literals(tid, template.get(frag))
    return normalise_ontology_datasource_names(query)


def standalone_query(template):
    if not template.get("result_columns"):
        raise ValueError(
            f"standalone materialised query '{template.get('id')}' must declare "
            f"result_columns (they define its typed storage table)"
        )
    cypher = (template.get("materialise") or {}).get("cypher")
    warn_on_unrecognised_datasource_literals(template.get("id") or "<unknown>", cypher)
    return normalise_ontology_datasource_names(cypher)


def query_to_run(template):
    """The Cypher to execute at dataload for a materialised template."""
    if is_standalone(template):
        return standalone_query(template)
    if is_parameterised(template):
        return derive_materialise_query(template)
    raise ValueError("template is not materialised")


def storage_columns(template):
    """The logical columns of this query's storage table, recorded in metadata.

    Full mode stores every result column (with its serving attributes); a
    counts_only histogram stores only the base column(s) plus `_count`. The
    physical (typed) schema is derived from these by the pgcopy writer.
    """
    cols = template.get("result_columns") or []
    if is_parameterised(template) and materialise_mode(template) == "counts_only":
        bases = set(_base_columns(template))
        out = [{"column_id": c.get("column_id"), "column_type": c.get("column_type")}
               for c in cols if c.get("column_id") in bases]
        out.append({"column_id": "_count", "column_type": "int"})
        return out
    out = []
    for c in cols:
        entry = {"column_id": c.get("column_id"), "column_type": c.get("column_type")}
        if c.get("optional"):
            entry["optional"] = True
        if c.get("facet"):
            entry["facet"] = True
        out.append(entry)
    return out


def serving_metadata(template):
    """Per-template serving directives recorded in graph_metadata so the API can
    route /query/{id} to the Postgres closure path.
    """
    if is_standalone(template):
        return {"kind": "standalone"}

    params_meta = []
    for p in closure_params(template):
        params_meta.append({
            "param_id": p["param_id"],
            "filters_column": filters_column(p),
            "closure": derived_matches(template, p),
            "param_type": p.get("param_type"),
        })
    return {
        "kind": "parameterised",
        "mode": materialise_mode(template),
        "params": params_meta,
        # How serving matches a stored row's base against the queried closure:
        # by the base node's id ("<col>_nid" = ANY(closure node ids)). Builds
        # that predate this key stored curie arrays instead and are served with
        # the older overlap predicate.
        "closure_key": "nid",
    }
