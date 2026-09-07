#!/usr/bin/env bash
# Bench harness: runs the redacter binary against the fixed corpus of text
# fixtures under test-fixtures/documents/ for one or more provider specs and
# records timings + redacted output under test-fixtures/bench-dlp/results/<provider>/.
#
# Usage:
#   ./run.sh <provider-spec>...
#
# A provider spec is a redacter -d value, or several joined with '+' to run
# them as a chain in one invocation, e.g.:
#   local-rules
#   local-ner
#   local-rules+local-ner
#   gcp-dlp
#   gcp-vertex-ai
#
# For each spec this runs, against the whole corpus directory:
#   1 warm-up pass (untimed, discarded) + 3 timed passes
# plus one timed pass per corpus file (per-file latency).
#
# A cloud provider (gcp-dlp, gcp-vertex-ai, or a chain containing either) is
# therefore invoked exactly (4 + <file count>) times per run.sh call, and this
# script prints that count and refuses to run more passes than that -- there
# is no retry loop and no flag to add extra passes for a cloud spec.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCUMENTS_DIR="$SCRIPT_DIR/../documents"
RESULTS_DIR="$SCRIPT_DIR/results"
GCP_PROJECT_ID="${GCP_PROJECT_ID:-latestbit}"

# The seven text-only fixtures that make up the corpus, read directly from
# test-fixtures/documents/ rather than a copy. This is an explicit list, not a
# directory listing, so that test-fixtures/documents/customer-form.pdf (and
# anything else added there later) never becomes part of the corpus -- see
# README.md for why the corpus is text-only.
CORPUS_FILE_NAMES=(
  customer-note.txt
  customer.json
  customer-profile.html
  customers.csv
  multilingual.txt
  false-positives-en.txt
  dates-en.txt
)

if [[ $# -eq 0 ]]; then
  echo "usage: $0 <provider-spec>... (e.g. local-rules local-ner local-rules+local-ner gcp-dlp gcp-vertex-ai)" >&2
  exit 1
fi

# Locate the release binary: explicit REDACTER_BIN wins, then
# $CARGO_TARGET_DIR/release/redacter, then the repo's default target dir.
REDACTER_BIN="${REDACTER_BIN:-}"
if [[ -z "$REDACTER_BIN" ]]; then
  for candidate in \
    "${CARGO_TARGET_DIR:-/nonexistent}/release/redacter" \
    "$SCRIPT_DIR/../../target/release/redacter" \
  ; do
    if [[ -x "$candidate" ]]; then
      REDACTER_BIN="$candidate"
      break
    fi
  done
fi
if [[ -z "$REDACTER_BIN" || ! -x "$REDACTER_BIN" ]]; then
  echo "error: could not find the redacter release binary." >&2
  echo "Build it first, e.g.:" >&2
  echo "  export CARGO_TARGET_DIR=/some/scratch/dir" >&2
  echo "  cargo build --release" >&2
  echo "then re-run with that CARGO_TARGET_DIR set, or pass REDACTER_BIN=/path/to/redacter." >&2
  exit 1
fi

CORPUS_FILES=()
for name in "${CORPUS_FILE_NAMES[@]}"; do
  path="$DOCUMENTS_DIR/$name"
  if [[ ! -f "$path" ]]; then
    echo "error: corpus fixture not found: $path" >&2
    exit 1
  fi
  CORPUS_FILES+=("$path")
done
FILE_COUNT=${#CORPUS_FILES[@]}

# Whole-corpus passes need a single directory to hand to `redacter cp`. Build
# one under $RESULTS_DIR (gitignored) out of hard links to the fixtures
# above -- not symlinks, since the local filesystem walker skips symlinks --
# so the corpus is read straight from test-fixtures/documents/ without a
# checked-in copy and without pulling in customer-form.pdf.
CORPUS_VIEW_DIR="$RESULTS_DIR/corpus-view"
rm -rf "$CORPUS_VIEW_DIR"
mkdir -p "$CORPUS_VIEW_DIR"
for f in "${CORPUS_FILES[@]}"; do
  ln "$f" "$CORPUS_VIEW_DIR/$(basename "$f")"
done

is_cloud_spec() {
  case "$1" in
    *gcp-dlp*|*gcp-vertex-ai*) return 0 ;;
    *) return 1 ;;
  esac
}

# Populates the global PROVIDER_FLAGS array with the -d/-provider-specific
# flags for a spec like "local-rules+local-ner".
build_provider_flags() {
  local spec="$1"
  PROVIDER_FLAGS=()
  local part
  local IFS='+'
  read -ra parts <<< "$spec"
  for part in "${parts[@]}"; do
    PROVIDER_FLAGS+=(-d "$part")
    if [[ "$part" == "gcp-dlp" || "$part" == "gcp-vertex-ai" ]]; then
      PROVIDER_FLAGS+=(--gcp-project-id "$GCP_PROJECT_ID")
    fi
  done
}

# invoke_redacter <label> <src> <out_dir> <provider_flags...>
# Runs the binary once, enforcing the per-provider call budget.
invoke_redacter() {
  local label="$1" src="$2" out_dir="$3"
  shift 3
  mkdir -p "$out_dir"
  "$REDACTER_BIN" cp "$@" --download-models no "$src" "$out_dir/" \
    >"$out_dir.log" 2>&1
  CALL_COUNT=$((CALL_COUNT + 1))
  if [[ "$CALL_COUNT" -gt "$CALL_BUDGET" ]]; then
    echo "error: $label exceeded its call budget ($CALL_BUDGET) -- aborting" >&2
    exit 1
  fi
}

for spec in "$@"; do
  echo "=== provider: $spec ==="
  build_provider_flags "$spec"

  out_root="$RESULTS_DIR/$spec"
  rm -rf "$out_root"
  mkdir -p "$out_root"

  CALL_COUNT=0
  CALL_BUDGET=$((1 + 3 + FILE_COUNT))

  if is_cloud_spec "$spec"; then
    echo "cloud provider: this run will make $CALL_BUDGET requests against the" \
         "corpus (1 warm-up + 3 timed whole-corpus passes + $FILE_COUNT per-file passes)."
  else
    echo "local provider: $CALL_BUDGET passes planned (no network calls)."
  fi

  # Warm-up pass (untimed, discarded output) -- loads models / establishes
  # client connections so the timed passes measure steady-state latency.
  invoke_redacter "$spec warm-up" "$CORPUS_VIEW_DIR" "$out_root/warmup" "${PROVIDER_FLAGS[@]}"

  # 3 timed whole-corpus passes.
  whole_ms=()
  for i in 1 2 3; do
    run_dir="$out_root/run$i"
    t0=$(date +%s%N)
    invoke_redacter "$spec run$i" "$CORPUS_VIEW_DIR" "$run_dir" "${PROVIDER_FLAGS[@]}"
    t1=$(date +%s%N)
    ms=$(( (t1 - t0) / 1000000 ))
    whole_ms+=("$ms")
    echo "  run$i: ${ms} ms"
  done

  # Per-file timed passes.
  per_file_json="{"
  first=1
  for f in "${CORPUS_FILES[@]}"; do
    name="$(basename "$f")"
    file_out="$out_root/perfile/$name"
    t0=$(date +%s%N)
    invoke_redacter "$spec perfile:$name" "$f" "$file_out" "${PROVIDER_FLAGS[@]}"
    t1=$(date +%s%N)
    ms=$(( (t1 - t0) / 1000000 ))
    echo "  perfile $name: ${ms} ms"
    if [[ "$first" -eq 1 ]]; then first=0; else per_file_json+=","; fi
    per_file_json+="\"$name\":$ms"
  done
  per_file_json+="}"

  whole_json="[$(IFS=,; echo "${whole_ms[*]}")]"
  printf '{"whole_corpus_ms":%s,"per_file_ms":%s,"calls_made":%s}\n' \
    "$whole_json" "$per_file_json" "$CALL_COUNT" \
    > "$out_root/timings.json"

  echo "provider $spec: $CALL_COUNT / $CALL_BUDGET calls made"
  echo
done

echo "Done. Results under $RESULTS_DIR/<provider>/. Run ./score.py to summarize."
