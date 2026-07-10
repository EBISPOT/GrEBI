#!/bin/bash
#
# Publish a completed GrEBI data release as a GitHub Release.
#
# This is the GitHub counterpart to ebi_datarelease_to_ftp.sh: it takes the
# directory produced by a Codon build (the same layout validated by
# check_datarelease.sh) and creates/updates a dated GitHub Release, uploading
# the release artefacts as assets.
#
# GitHub caps each release asset at 2 GiB. The Neo4j/Postgres store archives
# routinely exceed that, so oversized artefacts are handled in one of two ways:
#   - default: they are NOT uploaded; the release notes link to the FTP copy
#     (this script is normally run right after ebi_datarelease_to_ftp.sh).
#   - GREBI_GH_SPLIT=1: they are split into <2 GiB parts (release.tar.xz.partNN)
#     and every part is uploaded, with a reassembly note in the release body.
#
# Small artefacts (*_metadata.json, a packaged query_results.tar.xz, the run
# script, and a generated SHA256SUMS) are always uploaded so the release is
# self-describing even when the big stores live on FTP.
#
# Unlike ebi_datarelease_to_ftp.sh this does not need a datamover node — it only
# needs outbound HTTPS to api.github.com and a token, so it can run from a Codon
# login node.
#
# Requirements:
#   - curl, python3 (both present on Codon login nodes)
#   - GITHUB_TOKEN with `contents: write` on the target repo (a fine-grained PAT
#     or a classic PAT with `repo` scope)
#
# Environment overrides (all optional):
#   GREBI_GH_REPO         owner/repo to publish to        (default EBISPOT/GrEBI)
#   GREBI_RELEASE_VERSION release/tag version              (default: date +%Y-%b-%d)
#   GREBI_GH_TAG_PREFIX   prefix prepended to the tag      (default: data-)
#   GREBI_GH_TARGET       commit-ish the tag points at     (default: stable)
#   GREBI_GH_PRERELEASE   true|false                       (default: false)
#   GREBI_GH_SPLIT        1 to split & upload oversized archives instead of
#                         linking them to FTP               (default: unset)
#   GREBI_FTP_URL_BASE    public FTP base for oversized-archive links
#                         (default https://ftp.ebi.ac.uk/pub/databases/spot/kg)
#   GREBI_GH_DRY_RUN      1 to print the release plan and stop before the API
#   GREBI_GH_ASSET_LIMIT  per-asset byte cap                 (default 2 GiB)

set -Eeuo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <datarelease_path>"
  exit 1
fi

DATARELEASE_PATH=$1
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

if [ -z "${GITHUB_TOKEN:-}" ]; then
  echo "GITHUB_TOKEN is not set (need a token with contents:write on the repo)"
  exit 1
fi

REPO=${GREBI_GH_REPO:-EBISPOT/GrEBI}
VERSION=${GREBI_RELEASE_VERSION:-$(date +"%Y-%b-%d")}
TAG_PREFIX=${GREBI_GH_TAG_PREFIX:-data-}
TAG="${TAG_PREFIX}${VERSION}"
TARGET=${GREBI_GH_TARGET:-stable}
PRERELEASE=${GREBI_GH_PRERELEASE:-false}
FTP_URL_BASE=${GREBI_FTP_URL_BASE:-https://ftp.ebi.ac.uk/pub/databases/spot/kg}
ASSET_LIMIT=${GREBI_GH_ASSET_LIMIT:-$((2 * 1024 * 1024 * 1024))}   # GitHub's 2 GiB per-asset cap
SPLIT_SIZE=1900m                          # keeps every part comfortably < 2 GiB

API="https://api.github.com/repos/$REPO"
UPLOADS="https://uploads.github.com/repos/$REPO"
AUTH=(-H "Authorization: Bearer $GITHUB_TOKEN"
      -H "Accept: application/vnd.github+json"
      -H "X-GitHub-Api-Version: 2022-11-28")

# Reuse the shared validation so we fail early on an incomplete release.
"$SCRIPT_DIR/check_datarelease.sh" "$DATARELEASE_PATH"

# jq isn't guaranteed on Codon; use python3 (used elsewhere in the pipeline).
# Tolerant of empty/non-JSON input (e.g. a 404 body) so callers can probe.
json_get() {
  python3 -c '
import json, sys
try:
    print(json.load(sys.stdin).get(sys.argv[1], ""))
except Exception:
    pass
' "$1"
}

human() { numfmt --to=iec --suffix=B "$1" 2>/dev/null || echo "$1 bytes"; }

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

echo "Publishing data release '$VERSION' to $REPO as tag '$TAG'"

# --- Decide which artefacts to attach directly, split, or link to FTP ---------
declare -a UPLOAD_FILES=()   # files uploaded verbatim as assets
declare -a BODY_ATTACHED=()  # markdown lines describing attached assets
declare -a BODY_FTP=()       # markdown lines describing FTP-only artefacts

FTP_URL="$FTP_URL_BASE/$VERSION"

consider_archive() {
  # $1 = path to a (potentially large) archive already in the datarelease dir
  local f=$1
  local name size
  name=$(basename "$f")
  size=$(stat -c %s "$f")

  if [ "$size" -lt "$ASSET_LIMIT" ]; then
    UPLOAD_FILES+=("$f")
    BODY_ATTACHED+=("- \`$name\` ($(human "$size"))")
    return
  fi

  if [ "${GREBI_GH_SPLIT:-}" = "1" ]; then
    echo "Splitting oversized archive $name ($(human "$size")) into $SPLIT_SIZE parts"
    ( cd "$WORK_DIR" && split -b "$SPLIT_SIZE" -d "$f" "${name}.part" )
    local parts=("$WORK_DIR/${name}.part"*)
    UPLOAD_FILES+=("${parts[@]}")
    BODY_ATTACHED+=("- \`$name\` ($(human "$size")) — split into ${#parts[@]} parts; reassemble with \`cat ${name}.part* > $name\`")
  else
    BODY_FTP+=("- \`$name\` ($(human "$size")) — [download from FTP]($FTP_URL/$name)")
  fi
}

shopt -s nullglob

# Small, always-attached artefacts
for meta in "$DATARELEASE_PATH"/*_metadata.json; do
  UPLOAD_FILES+=("$meta")
  BODY_ATTACHED+=("- \`$(basename "$meta")\` ($(human "$(stat -c %s "$meta")"))")
done

# query_results/ is a directory — package it so it can be an asset
if [ -d "$DATARELEASE_PATH/query_results" ]; then
  QR_TGZ="$WORK_DIR/query_results.tar.xz"
  tar -C "$DATARELEASE_PATH" -cJf "$QR_TGZ" query_results
  consider_archive "$QR_TGZ"
fi

# The generated dev run script, if it was placed at the top level
if [ -f "$DATARELEASE_PATH/grebi_dev.sh" ]; then
  UPLOAD_FILES+=("$DATARELEASE_PATH/grebi_dev.sh")
  BODY_ATTACHED+=("- \`grebi_dev.sh\` — starts the GrEBI stack from the extracted release")
fi

# Large store archives (Neo4j per subgraph, Postgres, and the combined bundle).
# postgres.tar.xz / release.tar.xz are literal names (not globs), so guard with
# -e; the *_neo4j.tar.xz glob is covered by nullglob but -e is harmless there.
for arc in "$DATARELEASE_PATH"/*_neo4j.tar.xz "$DATARELEASE_PATH"/postgres.tar.xz "$DATARELEASE_PATH"/release.tar.xz; do
  [ -e "$arc" ] || continue
  consider_archive "$arc"
done

# --- Checksums over everything we are attaching -------------------------------
if [ "${#UPLOAD_FILES[@]}" -gt 0 ]; then
  SUMS="$WORK_DIR/SHA256SUMS"
  ( for f in "${UPLOAD_FILES[@]}"; do
      ( cd "$(dirname "$f")" && sha256sum "$(basename "$f")" )
    done ) > "$SUMS"
  UPLOAD_FILES+=("$SUMS")
fi

# --- Build the release body ---------------------------------------------------
BODY_FILE="$WORK_DIR/body.md"
{
  echo "GrEBI knowledge graph data release **$VERSION**."
  echo
  echo "Built by the GrEBI pipeline (\`dataload/nextflow/main.nf\`) on the EMBL-EBI Codon HPC."
  echo
  if [ "${#BODY_ATTACHED[@]}" -gt 0 ]; then
    echo "### Attached artefacts"
    printf '%s\n' "${BODY_ATTACHED[@]}"
    echo
  fi
  if [ "${#BODY_FTP[@]}" -gt 0 ]; then
    echo "### Large stores (hosted on FTP)"
    echo "These exceed GitHub's 2 GiB per-asset limit and are served from EBI FTP:"
    echo
    printf '%s\n' "${BODY_FTP[@]}"
    echo
    echo "All artefacts for this release: <$FTP_URL/> (or \`latest\`: <$FTP_URL_BASE/latest/>)"
    echo
  fi
  echo "Verify downloads with \`sha256sum -c SHA256SUMS\`."
} > "$BODY_FILE"

# --- Dry run: show the plan and stop before touching the API ------------------
if [ "${GREBI_GH_DRY_RUN:-}" = "1" ]; then
  echo
  echo "DRY RUN — would publish to $REPO:"
  echo "  tag:    $TAG"
  echo "  target: $TARGET"
  echo "  assets:"
  for f in "${UPLOAD_FILES[@]}"; do
    echo "    - $(basename "$f") ($(human "$(stat -c %s "$f")"))"
  done
  echo "  release body:"
  sed 's/^/    | /' "$BODY_FILE"
  exit 0
fi

# --- Replace any existing release for this tag (idempotent re-runs) -----------
EXISTING_ID=$(curl -fsSL "${AUTH[@]}" "$API/releases/tags/$TAG" 2>/dev/null | json_get id || true)
if [ -n "$EXISTING_ID" ]; then
  echo "Deleting existing release $EXISTING_ID for tag $TAG"
  curl -fsSL -X DELETE "${AUTH[@]}" "$API/releases/$EXISTING_ID" >/dev/null
fi

# --- Create the release -------------------------------------------------------
CREATE_PAYLOAD=$(python3 - "$TAG" "$TARGET" "$VERSION" "$PRERELEASE" "$BODY_FILE" <<'PY'
import json, sys
tag, target, version, prerelease, body_file = sys.argv[1:6]
with open(body_file) as fh:
    body = fh.read()
print(json.dumps({
    "tag_name": tag,
    "target_commitish": target,
    "name": f"KG data release {version}",
    "body": body,
    "draft": False,
    "prerelease": prerelease.lower() == "true",
}))
PY
)

# No -f here: on an HTTP error we want to capture the JSON body and surface it
# via the RELEASE_ID check below rather than aborting on curl's exit code.
RELEASE_JSON=$(printf '%s' "$CREATE_PAYLOAD" | curl -sSL -X POST "${AUTH[@]}" --data-binary @- "$API/releases")
RELEASE_ID=$(printf '%s' "$RELEASE_JSON" | json_get id)
RELEASE_URL=$(printf '%s' "$RELEASE_JSON" | json_get html_url)

if [ -z "$RELEASE_ID" ]; then
  echo "Failed to create release:"
  printf '%s\n' "$RELEASE_JSON"
  exit 1
fi

echo "Created release $RELEASE_ID: $RELEASE_URL"

# --- Upload assets ------------------------------------------------------------
for f in "${UPLOAD_FILES[@]}"; do
  name=$(basename "$f")
  echo "Uploading $name ($(human "$(stat -c %s "$f")"))"
  curl -fsSL -X POST "${AUTH[@]}" \
    -H "Content-Type: application/octet-stream" \
    --data-binary @"$f" \
    "$UPLOADS/releases/$RELEASE_ID/assets?name=$name" >/dev/null
done

echo "Done. Release available at: $RELEASE_URL"
