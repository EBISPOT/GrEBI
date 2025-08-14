# ncbitaxon_local_fast.py
from __future__ import annotations
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDFS, SKOS
from collections import defaultdict, deque
from typing import List, Dict, Set, Optional, Iterable, Tuple
import unicodedata
import os

DEFAULT_NCBITAXON_PATH = "ncbitaxon.owl"

OBO = Namespace("http://purl.obolibrary.org/obo/")
NCBITAXON_META = Namespace("http://purl.obolibrary.org/obo/ncbitaxon#")
OBOINOWL = Namespace("http://www.geneontology.org/formats/oboInOwl#")

SYN_PREDICATES = [
    NCBITAXON_META.synonym,
    SKOS.altLabel,
    OBOINOWL.hasExactSynonym,
    OBOINOWL.hasRelatedSynonym,
    OBOINOWL.hasNarrowSynonym,
    OBOINOWL.hasBroadSynonym,
]

def _norm(s: str) -> str:
    return unicodedata.normalize("NFKC", s).casefold().strip()

def _taxid_from_iri(iri: URIRef) -> Optional[str]:
    s = str(iri)
    idx = s.rfind("NCBITaxon_")
    if idx == -1:
        return None
    return s[idx + len("NCBITaxon_"):]

class NCBITaxonLocal:
    __slots__ = ("g","parents","labelsyn","name_to_ids","ancestors")
    def __init__(self):
        p = DEFAULT_NCBITAXON_PATH
        if not os.path.exists(p):
            raise FileNotFoundError(f"NCBITaxon file not found: {p}")
        print(f"Parsing {p}...")
        self.g = Graph().parse(p)
        self.parents: Dict[str, Tuple[str, ...]] = {}
        self.labelsyn: Dict[str, Tuple[str, ...]] = {}
        self.name_to_ids: Dict[str, Set[str]] = defaultdict(set)
        self.ancestors: Dict[str, Tuple[str, ...]] = {}
        self._build_indexes()
        self._materialize_ancestors()
        # self.g.close(); del self.g

    def _build_indexes(self) -> None:
        print("Building indexes...")
        parents_tmp: Dict[str, Set[str]] = defaultdict(set)
        labels_tmp: Dict[str, list] = defaultdict(list)
        syns_tmp: Dict[str, list] = defaultdict(list)

        print("  Collecting parents...")
        for i, (s, _, o) in enumerate(self.g.triples((None, RDFS.subClassOf, None)), 1):
            if i % 100_000 == 0:
                print(f"    processed {i} subClassOf triples...")
            sid = _taxid_from_iri(s) if isinstance(s, URIRef) else None
            if not sid:
                continue
            if isinstance(o, URIRef):
                pid = _taxid_from_iri(o)
                if pid:
                    parents_tmp[sid].add(pid)

        print("  Collecting labels...")
        for p in (RDFS.label, SKOS.prefLabel):
            for i, (s, _, o) in enumerate(self.g.triples((None, p, None)), 1):
                if i % 100_000 == 0:
                    print(f"    processed {i} label triples for {p}...")
                if not isinstance(o, Literal):
                    continue
                tid = _taxid_from_iri(s) if isinstance(s, URIRef) else None
                if not tid:
                    continue
                labels_tmp[tid].append(str(o))

        print("  Collecting synonyms...")
        for pred in SYN_PREDICATES:
            for i, (s, _, o) in enumerate(self.g.triples((None, pred, None)), 1):
                if i % 100_000 == 0:
                    print(f"    processed {i} synonym triples for {pred}...")
                if not isinstance(o, Literal):
                    continue
                tid = _taxid_from_iri(s) if isinstance(s, URIRef) else None
                if not tid:
                    continue
                syns_tmp[tid].append(str(o))

        def _dedup(seq: Iterable[str]) -> list[str]:
            seen = set(); out = []
            for x in seq:
                k = _norm(x)
                if k not in seen:
                    seen.add(k); out.append(x)
            return out

        print("  Finalizing label/synonym and reverse index...")
        all_ids = set().union(labels_tmp.keys(), syns_tmp.keys(), parents_tmp.keys())
        for idx, tid in enumerate(all_ids, 1):
            if idx % 50_000 == 0:
                print(f"    finalized {idx} taxa...")
            lab = _dedup(labels_tmp.get(tid, ()))
            syn = _dedup(syns_tmp.get(tid, ()))
            seen = {_norm(x) for x in lab}
            merged = lab[:]
            for s in syn:
                k = _norm(s)
                if k not in seen:
                    seen.add(k); merged.append(s)
            self.labelsyn[tid] = tuple(merged)
            for name in merged:
                self.name_to_ids[_norm(name)].add(tid)
            self.parents[tid] = tuple(parents_tmp.get(tid, ()))

    def _materialize_ancestors(self) -> None:
        print("Precomputing ancestors...")
        for idx, tid in enumerate(self.parents.keys() | self.labelsyn.keys(), 1):
            if idx % 50_000 == 0:
                print(f"  computed ancestors for {idx} taxa...")
            if tid in self.ancestors:
                continue
            self._bfs_ancestors_fill(tid)

    def _bfs_ancestors_fill(self, tid: str) -> Tuple[str, ...]:
        if tid in self.ancestors:
            return self.ancestors[tid]
        out: List[str] = []
        seen: Set[str] = set()
        q = deque(self.parents.get(tid, ()))
        while q:
            p = q.popleft()
            if p in seen:
                continue
            seen.add(p)
            out.append(p)
            cached = self.ancestors.get(p)
            if cached is not None:
                for c in cached:
                    if c not in seen:
                        seen.add(c); out.append(c)
                continue
            for gp in self.parents.get(p, ()):
                if gp not in seen:
                    q.append(gp)
        self.ancestors[tid] = tuple(out)
        return self.ancestors[tid]

    def get_labels_and_synonyms(self, taxid: str | int) -> List[str]:
        return list(self.labelsyn.get(str(taxid), ()))

    def get_ancestors(self, taxid: str | int) -> List[str]:
        return list(self.ancestors.get(str(taxid), ()))

    def get_by_name(self, name: str) -> List[str]:
        return sorted(self.name_to_ids.get(_norm(name), []))

_NCBI: Optional[NCBITaxonLocal] = None

def load_ncbitaxon() -> None:
    global _NCBI
    _NCBI = NCBITaxonLocal()

def _require_loaded() -> NCBITaxonLocal:
    if _NCBI is None:
        raise RuntimeError("NCBITaxon graph not loaded. Call load_ncbitaxon() once at startup.")
    return _NCBI

def get_labels_and_synonyms(taxid: str | int) -> List[str]:
    return _require_loaded().get_labels_and_synonyms(taxid)

def get_ancestors(taxid: str | int) -> List[str]:
    return _require_loaded().get_ancestors(taxid)

def get_by_name(name: str) -> List[str]:
    return _require_loaded().get_by_name(name)
