#!/usr/bin/env -S uv run --quiet
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "bioregistry==0.14.6",
#     "requests",
#     "pillow",
#     "brotli",
# ]
# ///
"""Generate the data behind the outbound database links on node pages.

Writes
  src/db_links/db_links.json   a URL pattern (and, where known, an id pattern)
                               per Bioregistry prefix, the curated database
                               table, regex rules for ids that carry no
                               prefix, and the datasource -> database map
  dist/db_icons/<db>.png|svg   one icon per curated database

Bioregistry provides the long tail. The curated tables below route EBI-hosted
identifiers to their EBI home (OLS for ontology terms) and give the databases
that matter an icon. Nothing here touches the dataload's prefix maps, so a
regeneration only ever needs a UI rebuild, never a data load.

Usage, from webapp/grebi_ui:
    uv run scripts/make_db_links.py             # JSON and icons
    uv run scripts/make_db_links.py --no-icons  # JSON only, keeping the icons already present
    uv run scripts/make_db_links.py --offline   # as --no-icons, and trust Bioregistry's OLS routes unchecked
    uv run scripts/make_db_links.py --check     # also GET every curated URL with an example id
"""
import argparse
import datetime
import io
import json
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

import bioregistry
import requests
from PIL import Image

UI = Path(__file__).resolve().parent.parent
JSON_OUT = UI / "src" / "db_links" / "db_links.json"
ICON_DIR = UI / "dist" / "db_icons"
ICON_SIZE = 32
# brotli is a dependency because requests only decodes br-encoded responses (Ensembl) with it installed
UA = {"User-Agent": "Mozilla/5.0 (compatible; GrEBI db_links generator)"}

EBI_FAVICON = "https://ebi.emblstatic.net/web_guidelines/EBI-Framework/v1.4/images/logos/EMBL-EBI/favicons/favicon-32x32.png"

# database key -> (name, homepage, icon URL, hosted at EBI). An icon URL of None means look for one
# on the homepage; "" means no icon (the homepage's icon is not the database's).
DATABASES = {
    "ebi": ("EMBL-EBI", "https://www.ebi.ac.uk", EBI_FAVICON, True),
    "ols": ("Ontology Lookup Service", "https://www.ebi.ac.uk/ols4", "https://www.ebi.ac.uk/ols4/logo192.png", True),
    "chebi": ("ChEBI", "https://www.ebi.ac.uk/chebi", "https://www.ebi.ac.uk/chebi/chebi_icon_only_rgb.png", True),
    "chembl": ("ChEMBL", "https://www.ebi.ac.uk/chembl", "https://www.ebi.ac.uk/chembl/favicon.ico", True),
    "ensembl": ("Ensembl", "https://www.ensembl.org", "https://www.ensembl.org/static/images/favicon.a94e5097d6f2285305b4.svg", True),
    "uniprot": ("UniProt", "https://www.uniprot.org", "https://www.uniprot.org/favicon-32x32.png", True),
    "unichem": ("UniChem", "https://www.ebi.ac.uk/unichem", EBI_FAVICON, True),
    "pdbe": ("PDBe", "https://www.ebi.ac.uk/pdbe", "https://www.ebi.ac.uk/pdbe/sites/default/files/pdbe_0_0.png", True),
    "reactome": ("Reactome", "https://reactome.org", "https://reactome.org/templates/favourite/images/logo/icon.png", True),
    "gwas": ("GWAS Catalog", "https://www.ebi.ac.uk/gwas", "https://www.ebi.ac.uk/gwas/images/favicon.ico", True),
    "europepmc": ("Europe PMC", "https://europepmc.org", "https://europepmc.org/favicon.ico", True),
    "hgnc": ("HGNC", "https://www.genenames.org", "https://www.genenames.org/img/hgnc/favicon/favicon.svg", True),
    "impc": ("IMPC", "https://www.mousephenotype.org", "https://www.mousephenotype.org/favicon.ico", True),
    "mgnify": ("MGnify", "https://www.ebi.ac.uk/metagenomics", EBI_FAVICON, True),
    "opentargets": ("Open Targets", "https://platform.opentargets.org", "https://platform.opentargets.org/favicon.png", True),
    "biostudies": ("BioStudies", "https://www.ebi.ac.uk/biostudies", EBI_FAVICON, True),
    "pride": ("PRIDE", "https://www.ebi.ac.uk/pride", EBI_FAVICON, True),
    "gxa": ("Expression Atlas", "https://www.ebi.ac.uk/gxa", "https://www.ebi.ac.uk/gxa/resources/favicons/favicon-32x32.png", True),
    "metabolights": ("MetaboLights", "https://www.ebi.ac.uk/metabolights", "https://www.ebi.ac.uk/metabolights/mtbls_favicon.png", True),
    "interpro": ("InterPro", "https://www.ebi.ac.uk/interpro", EBI_FAVICON, True),
    # not hosted at EBI: their favicons
    "ncbi": ("NCBI", "https://www.ncbi.nlm.nih.gov", "https://www.ncbi.nlm.nih.gov/favicon.ico", False),
    "pubchem": ("PubChem", "https://pubchem.ncbi.nlm.nih.gov", "https://pubchem.ncbi.nlm.nih.gov/favicon.ico", False),
    "drugbank": ("DrugBank", "https://go.drugbank.com", "https://go.drugbank.com/favicon.ico", False),
    "kegg": ("KEGG", "https://www.kegg.jp", "https://www.kegg.jp/favicon.ico", False),
    "who": ("WHO ICD", "https://icd.who.int", "https://icd.who.int/img/icd11icon.png", False),
    "cas": ("CAS Common Chemistry", "https://commonchemistry.cas.org", "https://commonchemistry.cas.org/favicon.png", False),
    "fda": ("FDA", "https://precision.fda.gov", "https://precision.fda.gov/favicon.png", False),
    "bioportal": ("BioPortal", "https://bioportal.bioontology.org", "https://bioportal.bioontology.org/fav.ico", False),
    "nlm": ("NLM", "https://www.nlm.nih.gov", "https://www.nlm.nih.gov/favicon.ico", False),
    "mesh": ("MeSH", "https://meshb.nlm.nih.gov", None, False),
    "omim": ("OMIM", "https://omim.org", None, False),
    "wikidata": ("Wikidata", "https://www.wikidata.org", "https://www.wikidata.org/static/favicon/wikidata.ico", False),
    "doi": ("DOI", "https://doi.org", None, False),
    "monarch": ("Monarch Initiative", "https://monarchinitiative.org", None, False),
    "mgi": ("MGI", "https://www.informatics.jax.org", "https://www.informatics.jax.org/favicon.ico", False),
    "ctd": ("CTD", "https://ctdbase.org", None, False),
    "hmdb": ("HMDB", "https://www.hmdb.ca", None, False),
    "gtopdb": ("Guide to Pharmacology", "https://www.guidetopharmacology.org", None, False),
    "string": ("STRING", "https://string-db.org", "https://string-db.org/favicon.ico", False),
    "orphanet": ("Orphanet", "https://www.orpha.net", None, False),
    "aopwiki": ("AOP-Wiki", "https://aopwiki.org", "https://aopwiki.org/favicon.ico", False),
    "robokop": ("ROBOKOP", "https://robokop.renci.org", "https://robokop.renci.org/favicon.ico", False),
    "primekg": ("PrimeKG", "https://zitniklab.hms.harvard.edu/projects/PrimeKG", "", False),
    "hra": ("Human Reference Atlas", "https://humanatlas.io", "https://humanatlas.io/favicon.ico", False),
    "ubergraph": ("Ubergraph", "https://github.com/INCATools/ubergraph", None, False),
    "sssom": ("SSSOM", "https://mapping-commons.github.io/sssom", None, False),
    "biomappings": ("Biomappings", "https://biomappings.github.io/biomappings", None, False),
    "dismech": ("DisMech", "https://github.com/EBISPOT/DisMech", None, False),
}

# Bioregistry prefix -> (database key, URL pattern, $1 = the local id). EBI-first routes, and a
# few pages that are better than Bioregistry's default. Prefixes Bioregistry does not know
# (GrEBI's own spellings) are added as well.
PREFIX_OVERRIDES = {
    "chebi": ("chebi", "https://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI:$1"),
    "chembl": ("chembl", "https://www.ebi.ac.uk/chembl/id_lookup/$1"),
    "chembl.compound": ("chembl", "https://www.ebi.ac.uk/chembl/explore/compound/$1"),
    "chembl.target": ("chembl", "https://www.ebi.ac.uk/chembl/explore/target/$1"),
    "ensembl": ("ensembl", "https://www.ensembl.org/id/$1"),
    "uniprot": ("uniprot", "https://www.uniprot.org/uniprotkb/$1/entry"),
    "uniprot.isoform": ("uniprot", "https://www.uniprot.org/uniprotkb/$1/entry"),
    "pdb": ("pdbe", "https://www.ebi.ac.uk/pdbe/entry/pdb/$1"),
    "pdb.ligand": ("pdbe", "https://www.ebi.ac.uk/pdbe-srv/pdbechem/chemicalCompound/show/$1"),
    "reactome": ("reactome", "https://reactome.org/content/detail/$1"),
    "pubmed": ("europepmc", "https://europepmc.org/abstract/MED/$1"),
    "pmc": ("europepmc", "https://europepmc.org/article/PMC/$1"),
    "hgnc": ("hgnc", "https://www.genenames.org/data/gene-symbol-report/#!/hgnc_id/HGNC:$1"),
    "hgnc.symbol": ("hgnc", "https://www.genenames.org/data/gene-symbol-report/#!/symbol/$1"),
    "pride": ("pride", "https://www.ebi.ac.uk/pride/archive/projects/$1"),
    "pride.project": ("pride", "https://www.ebi.ac.uk/pride/archive/projects/$1"),
    "biostudies": ("biostudies", "https://www.ebi.ac.uk/biostudies/studies/$1"),
    "arrayexpress": ("biostudies", "https://www.ebi.ac.uk/biostudies/arrayexpress/studies/$1"),
    "gxa.expt": ("gxa", "https://www.ebi.ac.uk/gxa/experiments/$1"),
    "metabolights": ("metabolights", "https://www.ebi.ac.uk/metabolights/$1"),
    "mgnify.proj": ("mgnify", "https://www.ebi.ac.uk/metagenomics/studies/$1"),
    "mgnify.samp": ("mgnify", "https://www.ebi.ac.uk/metagenomics/samples/$1"),
    "interpro": ("interpro", "https://www.ebi.ac.uk/interpro/entry/InterPro/$1"),
    "pfam": ("interpro", "https://www.ebi.ac.uk/interpro/entry/pfam/$1"),
    "inchikey": ("unichem", "https://www.ebi.ac.uk/unichem/compoundsources?type=inchikey&compound=$1"),
    "ena.embl": ("ebi", "https://www.ebi.ac.uk/ena/browser/view/$1"),
    "intact": ("ebi", "https://www.ebi.ac.uk/intact/search?query=$1"),
    "complexportal": ("ebi", "https://www.ebi.ac.uk/complexportal/complex/$1"),
    "gtopdb": ("gtopdb", "https://www.guidetopharmacology.org/GRAC/LigandDisplayForward?ligandId=$1"),
    "iuphar.ligand": ("gtopdb", "https://www.guidetopharmacology.org/GRAC/LigandDisplayForward?ligandId=$1"),
    "ncbigene": ("ncbi", "https://www.ncbi.nlm.nih.gov/gene/$1"),
    "dbsnp": ("ncbi", "https://www.ncbi.nlm.nih.gov/snp/$1"),
    "omim": ("omim", "https://omim.org/entry/$1"),
    "omim.ps": ("omim", "https://omim.org/phenotypicSeries/PS$1"),
    "mgi": ("mgi", "https://www.informatics.jax.org/marker/MGI:$1"),
}

# (regex over the whole id, database key, URL with $0 = the id) for ids that carry no prefix
RULES = [
    (r"^GCST\d+$", "gwas", "https://www.ebi.ac.uk/gwas/studies/$0"),
    (r"^rs\d+$", "gwas", "https://www.ebi.ac.uk/gwas/variants/$0"),
    (r"^ENS[A-Z]{0,5}[GTPER]\d{11}(?:\.\d+)?$", "ensembl", "https://www.ensembl.org/id/$0"),
    (r"^PXD\d{6}$", "pride", "https://www.ebi.ac.uk/pride/archive/projects/$0"),
    (r"^S-[A-Z]{4}\d+$", "biostudies", "https://www.ebi.ac.uk/biostudies/studies/$0"),
    (r"^E-[A-Z]{4}-\d+$", "gxa", "https://www.ebi.ac.uk/gxa/experiments/$0"),
    (r"^MTBLS\d+$", "metabolights", "https://www.ebi.ac.uk/metabolights/$0"),
    (r"^MGYS\d+$", "mgnify", "https://www.ebi.ac.uk/metagenomics/studies/$0"),
    (r"^CHEMBL\d+$", "chembl", "https://www.ebi.ac.uk/chembl/id_lookup/$0"),
    (r"^(?:[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9](?:[A-Z][A-Z0-9]{2}[0-9]){1,2})(?:-\d+)?$", "uniprot", "https://www.uniprot.org/uniprotkb/$0/entry"),
]
RULE_EXAMPLES = ["GCST000187", "rs7903146", "ENSG00000146648", "PXD000001", "S-BSST1", "E-MTAB-513", "MTBLS1", "MGYS00000410", "CHEMBL1222250", "P00533"]

# host of a Bioregistry URL pattern -> database key, which decides the icon of everything not curated above
HOST_DB = {
    "www.ebi.ac.uk": "ebi", "ebi.ac.uk": "ebi", "europepmc.org": "europepmc",
    "www.ensembl.org": "ensembl", "ensembl.org": "ensembl",
    "www.uniprot.org": "uniprot", "purl.uniprot.org": "uniprot", "rest.uniprot.org": "uniprot",
    "reactome.org": "reactome", "www.reactome.org": "reactome", "www.genenames.org": "hgnc",
    "www.mousephenotype.org": "impc", "platform.opentargets.org": "opentargets",
    "www.ncbi.nlm.nih.gov": "ncbi", "pubchem.ncbi.nlm.nih.gov": "pubchem",
    "go.drugbank.com": "drugbank", "www.drugbank.ca": "drugbank", "www.kegg.jp": "kegg", "www.genome.jp": "kegg",
    "icd.who.int": "who", "commonchemistry.cas.org": "cas", "precision.fda.gov": "fda",
    "bioportal.bioontology.org": "bioportal", "purl.bioontology.org": "bioportal",
    "uts.nlm.nih.gov": "nlm", "mor.nlm.nih.gov": "nlm", "meshb.nlm.nih.gov": "mesh", "id.nlm.nih.gov": "mesh",
    "omim.org": "omim", "www.omim.org": "omim", "www.wikidata.org": "wikidata", "doi.org": "doi", "dx.doi.org": "doi",
    "monarchinitiative.org": "monarch", "www.informatics.jax.org": "mgi", "ctdbase.org": "ctd",
    "www.hmdb.ca": "hmdb", "hmdb.ca": "hmdb", "www.guidetopharmacology.org": "gtopdb", "string-db.org": "string",
    "www.orpha.net": "orphanet", "aopwiki.org": "aopwiki",
}

# GrEBI datasource id -> database key, or (key, homepage). A key ending in "." is a family
# (OLS.mondo, Robokop.Pharos, ...); "$1" in its homepage is the rest of the datasource id.
DATASOURCES = {
    "OLS.": ("ols", "https://www.ebi.ac.uk/ols4/ontologies/$1"),
    "Ontologies.": "ols",
    "Ontologies": "ols",
    "ols_top_k": "ols",
    "EFO.mappings": ("ols", "https://www.ebi.ac.uk/ols4/ontologies/efo"),
    "GWAS": "gwas", "ChEMBL": "chembl", "UniProt": "uniprot", "PDBe": "pdbe", "Reactome": "reactome",
    "HGNC": "hgnc", "IMPC": "impc", "PRIDE": "pride", "BioStudies": "biostudies", "ExpressionAtlas": "gxa",
    "Metabolights": "metabolights", "MGnify": "mgnify", "OpenTargets": "opentargets",
    "CTD": "ctd", "MeSH": "mesh", "MedGen": ("ncbi", "https://www.ncbi.nlm.nih.gov/medgen"),
    "MONARCH": "monarch", "PrimeKG": "primekg", "AOPWiki": "aopwiki", "UberGraph": "ubergraph",
    "SSSOM": "sssom", "Biomappings": "biomappings", "HRA": "hra", "DisMech": "dismech",
    "Robokop.": "robokop",
}


def ols4_ontologies():
    """The ids of the ontologies OLS4 serves, or None when the list cannot be fetched."""
    try:
        r = requests.get("https://www.ebi.ac.uk/ols4/api/ontologies?size=2000", headers=UA, timeout=60)
        r.raise_for_status()
        return {o["ontologyId"].lower() for o in r.json()["_embedded"]["ontologies"]}
    except Exception as e:
        print(f"  could not list OLS4 ontologies ({e.__class__.__name__}); trusting Bioregistry's OLS routes", file=sys.stderr)
        return None


def ols4_template(prefix, ols4_ids):
    """The OLS4 class page for a prefix OLS hosts, with $1 for the local id, else None."""
    ols = bioregistry.get_ols_prefix(prefix)
    if not ols or (ols4_ids is not None and ols.lower() not in ols4_ids):
        # Bioregistry lists a few ontologies (pr, vario, ...) that OLS4 does not serve
        return None
    marker = "GREBIMARKER"
    legacy = bioregistry.get_ols_iri(prefix, marker)
    if not legacy or legacy.count(marker) != 1:
        return None
    m = re.match(r"https://www\.ebi\.ac\.uk/ols/ontologies/([^/]+)/terms\?iri=(.*)$", legacy)
    if not m:
        return None
    return f"https://www.ebi.ac.uk/ols4/ontologies/{m.group(1)}/classes?iri={m.group(2)}".replace(marker, "$1")


def js_pattern(pattern):
    """Bioregistry's id pattern if a browser can run it too."""
    if not pattern or "(?P<" in pattern or re.search(r"\(\?[a-zA-Z]+\)", pattern):
        return None
    try:
        re.compile(pattern)
    except re.error:
        return None
    return pattern


def build_prefixes(ols4_ids):
    prefixes = {}
    for prefix in sorted(bioregistry.read_registry()):
        default = bioregistry.get_uri_format(prefix)
        db = None
        if prefix in PREFIX_OVERRIDES:
            db, url = PREFIX_OVERRIDES[prefix]
        elif (ols := ols4_template(prefix, ols4_ids)):
            db, url = "ols", ols
        elif default:
            url = default
            db = HOST_DB.get(urlparse(default).netloc)
        else:
            continue
        entry = {"name": bioregistry.get_name(prefix) or prefix, "url": url}
        if (pattern := js_pattern(bioregistry.get_pattern(prefix))):
            entry["pattern"] = pattern
        if db:
            entry["db"] = db
        prefixes[prefix] = entry
    for prefix, (db, url) in PREFIX_OVERRIDES.items():
        if prefix not in prefixes:
            prefixes[prefix] = {"name": DATABASES[db][0], "url": url, "db": db}
    return dict(sorted(prefixes.items()))


def discover_icon(homepage):
    """The best icon a page declares in its head, as an absolute URL."""
    if urlparse(homepage).netloc.endswith("github.com"):
        return None  # that would be GitHub's icon, not the project's
    try:
        r = requests.get(homepage, headers=UA, timeout=20)
        r.raise_for_status()
    except Exception:
        return None
    best, best_score = None, -1
    for tag in re.findall(r"<link[^>]+>", r.text, re.I):
        rel = re.search(r"rel=[\"']([^\"']+)", tag, re.I)
        href = re.search(r"href=[\"']([^\"']+)", tag, re.I)
        if not rel or not href or "icon" not in rel.group(1).lower():
            continue
        sizes = re.search(r"sizes=[\"'](\d+)", tag, re.I)
        score = int(sizes.group(1)) if sizes else (500 if href.group(1).endswith(".svg") else 16)
        if score > best_score:
            best, best_score = urljoin(r.url, href.group(1)), score
    return best


def save_icon(key, url):
    """Fetch an icon and store it as dist/db_icons/<key>.svg or a 32px PNG; the filename, or None."""
    r = requests.get(url, headers=UA, timeout=20)
    r.raise_for_status()
    content, ctype = r.content, r.headers.get("content-type", "")
    if "svg" in ctype or b"<svg" in content[:2000]:
        (ICON_DIR / f"{key}.svg").write_bytes(content)
        return f"{key}.svg"
    if not content or "text/html" in ctype:
        raise ValueError("not an image")
    img = Image.open(io.BytesIO(content))
    if img.format == "ICO":
        img = img.ico.getimage(max(img.ico.sizes()))
    img = img.convert("RGBA")
    img.thumbnail((ICON_SIZE, ICON_SIZE), Image.LANCZOS)
    canvas = Image.new("RGBA", (ICON_SIZE, ICON_SIZE), (0, 0, 0, 0))
    canvas.paste(img, ((ICON_SIZE - img.width) // 2, (ICON_SIZE - img.height) // 2))
    canvas.save(ICON_DIR / f"{key}.png", optimize=True)
    return f"{key}.png"


def build_databases(fetch_icons):
    ICON_DIR.mkdir(parents=True, exist_ok=True)
    databases, by_url = {}, {}
    for key, (name, homepage, icon_url, ebi) in DATABASES.items():
        entry = {"name": name, "homepage": homepage}
        if ebi:
            entry["ebi"] = True
        icon = None
        if icon_url == "":
            pass
        elif fetch_icons:
            for candidate in [icon_url, None if icon_url else discover_icon(homepage)]:
                if not candidate:
                    continue
                if candidate in by_url:
                    icon = by_url[candidate]
                    break
                try:
                    icon = by_url[candidate] = save_icon(key, candidate)
                    break
                except Exception as e:
                    print(f"  icon for {key}: {candidate} failed ({e.__class__.__name__}: {e})", file=sys.stderr)
            if not icon and icon_url:
                found = discover_icon(homepage)
                if found and found != icon_url:
                    try:
                        icon = by_url[found] = save_icon(key, found)
                    except Exception as e:
                        print(f"  icon for {key}: {found} failed ({e.__class__.__name__}: {e})", file=sys.stderr)
        else:
            for ext in ("svg", "png"):
                if (ICON_DIR / f"{key}.{ext}").exists():
                    icon = f"{key}.{ext}"
                    break
        if icon:
            entry["icon"] = icon
        databases[key] = entry
    ebi_icon = databases["ebi"].get("icon")
    for key, (name, homepage, icon_url, ebi) in DATABASES.items():
        if ebi and ebi_icon and "icon" not in databases[key]:
            databases[key]["icon"] = ebi_icon
    return databases


def build_datasources():
    out = {}
    for ds, value in DATASOURCES.items():
        db, homepage = (value, None) if isinstance(value, str) else value
        entry = {"db": db}
        if homepage:
            entry["homepage"] = homepage
        out[ds] = entry
    return out


def check_urls(prefixes):
    print("checking curated URLs")
    todo = []
    for prefix, (db, url) in PREFIX_OVERRIDES.items():
        example = bioregistry.get_example(prefix) or {"gtopdb": "4536", "chebi": "17234", "pubmed": "40323307"}.get(prefix)
        if example:
            todo.append((prefix, url.replace("$1", example)))
    for (pattern, db, url), example in zip(RULES, RULE_EXAMPLES):
        assert re.match(pattern, example), f"rule example {example} does not match {pattern}"
        todo.append((pattern, url.replace("$0", example)))
    for what, url in todo:
        try:
            code = requests.get(url, headers=UA, timeout=30).status_code
        except Exception as e:
            code = e.__class__.__name__
        print(f"  {code}  {what:24} {url}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-icons", action="store_true", help="do not fetch icons; keep the files already present")
    ap.add_argument("--offline", action="store_true", help="no network at all: as --no-icons, and Bioregistry's OLS routes go unchecked")
    ap.add_argument("--check", action="store_true", help="GET every curated URL with an example id")
    args = ap.parse_args()

    for pattern, db, url in RULES:
        re.compile(pattern)
        assert db in DATABASES, db
    for prefix, (db, url) in PREFIX_OVERRIDES.items():
        assert db in DATABASES, (prefix, db)
    for ds, value in DATASOURCES.items():
        assert (value if isinstance(value, str) else value[0]) in DATABASES, ds

    prefixes = build_prefixes(None if args.offline else ols4_ontologies())
    databases = build_databases(fetch_icons=not (args.no_icons or args.offline))
    data = {
        "_meta": {
            "generator": "scripts/make_db_links.py",
            "bioregistry": bioregistry.__version__ if hasattr(bioregistry, "__version__") else __import__("importlib.metadata").metadata.version("bioregistry"),
            "generated": datetime.date.today().isoformat(),
        },
        "databases": databases,
        "prefixes": prefixes,
        "rules": [{"regex": pattern, "db": db, "url": url} for pattern, db, url in RULES],
        "datasources": build_datasources(),
    }
    JSON_OUT.parent.mkdir(parents=True, exist_ok=True)
    with JSON_OUT.open("w") as f:
        # one prefix per line keeps the diffs of a regeneration readable
        f.write("{\n")
        for i, (section, value) in enumerate(data.items()):
            comma = "," if i < len(data) - 1 else ""
            if section == "prefixes":
                f.write('  "prefixes": {\n')
                items = list(value.items())
                for j, (k, v) in enumerate(items):
                    f.write(f"    {json.dumps(k)}: {json.dumps(v, ensure_ascii=False)}{',' if j < len(items) - 1 else ''}\n")
                f.write("  }" + comma + "\n")
            else:
                body = json.dumps(value, indent=2, ensure_ascii=False).replace("\n", "\n  ")
                f.write(f"  {json.dumps(section)}: {body}{comma}\n")
        f.write("}\n")
    print(f"wrote {JSON_OUT.relative_to(UI)}: {len(prefixes)} prefixes, {len(databases)} databases, "
          f"{sum(1 for d in databases.values() if d.get('icon'))} with an icon, {JSON_OUT.stat().st_size // 1024} KB")
    if args.check:
        check_urls(prefixes)


if __name__ == "__main__":
    main()
