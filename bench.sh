#!/usr/bin/env bash
# bench.sh — run all benchmarks and produce a summary.
#
# Usage:
#   ./bench.sh                    # sequential, all packages
#   ./bench.sh -parallel          # packages run in parallel (faster, noisier)
#   ./bench.sh table              # only the table package
#   ./bench.sh -count=3           # override iteration count (default 5)
#   ./bench.sh -filter=Join       # only benchmarks matching "Join"
#   ./bench.sh -parallel table csv  # parallel subset
#
# Output is saved to bench_latest.txt in the repo root.
# Requires Go (go test) to be on PATH.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
OUT="$ROOT/bench_latest.txt"

# ── Defaults ──────────────────────────────────────────────────────────────────
COUNT=5
FILTER="."
PARALLEL=false
PACKAGES=("./table/" "./csv/" "./etl/" "./schema/")

# ── Argument parsing ──────────────────────────────────────────────────────────
for arg in "$@"; do
  case "$arg" in
    -count=*)    COUNT="${arg#-count=}" ;;
    -filter=*)   FILTER="${arg#-filter=}" ;;
    -parallel)   PARALLEL=true ;;
    table)       PACKAGES=("./table/") ;;
    csv)         PACKAGES=("./csv/") ;;
    etl)         PACKAGES=("./etl/") ;;
    schema)      PACKAGES=("./schema/") ;;
    excel)       PACKAGES=("./excel/") ;;
    all)         PACKAGES=("./table/" "./csv/" "./etl/" "./schema/") ;;
    *)
      echo "Unknown argument: $arg" >&2
      echo "Usage: $0 [table|csv|etl|schema|all] [-parallel] [-count=N] [-filter=PATTERN]" >&2
      exit 1
      ;;
  esac
done

echo "┌─────────────────────────────────────────────────────────────────────────────────────────────────"
echo "│ gseq-table benchmark run"
echo "│ packages  : ${PACKAGES[*]}"
echo "│ -count    : $COUNT"
echo "│ -bench    : $FILTER"
echo "│ -parallel : $PARALLEL"
echo "│ output    : $OUT"
echo "└─────────────────────────────────────────────────────────────────────────────────────────────────"
echo ""

cd "$ROOT"

# ── Sequential run ────────────────────────────────────────────────────────────
if [ "$PARALLEL" = false ]; then
  go test \
    -bench="$FILTER" \
    -benchmem \
    -count="$COUNT" \
    -timeout=30m \
    "${PACKAGES[@]}" \
    2>&1 | tee "$OUT"

# ── Parallel run ──────────────────────────────────────────────────────────────
else
  echo "⚡ Running packages in parallel (results may be noisier)"
  echo ""

  TMPDIR_BENCH="$(mktemp -d)"
  trap 'rm -rf "$TMPDIR_BENCH"' EXIT

  PIDS=()
  PKG_NAMES=()

  for pkg in "${PACKAGES[@]}"; do
    # Derive a safe filename from the package path (strip ./ and /)
    safe="${pkg//\//}"
    safe="${safe//.}"
    tmpfile="$TMPDIR_BENCH/${safe}.txt"
    PKG_NAMES+=("$pkg → $tmpfile")

    go test \
      -bench="$FILTER" \
      -benchmem \
      -count="$COUNT" \
      -timeout=30m \
      "$pkg" \
      >"$tmpfile" 2>&1 &

    PIDS+=($!)
    echo "  started $pkg (pid $!)"
  done

  echo ""

  # Wait for all and report failures
  FAILED=0
  for i in "${!PIDS[@]}"; do
    pid="${PIDS[$i]}"
    pkg="${PACKAGES[$i]}"
    if wait "$pid"; then
      echo "  ✓ $pkg done"
    else
      echo "  ✗ $pkg FAILED (exit $?)" >&2
      FAILED=1
    fi
  done

  echo ""

  # Merge all per-package files into bench_latest.txt and print them
  : >"$OUT"
  for pkg in "${PACKAGES[@]}"; do
    safe="${pkg//\//}"
    safe="${safe//.}"
    tmpfile="$TMPDIR_BENCH/${safe}.txt"
    if [ -f "$tmpfile" ]; then
      cat "$tmpfile" | tee -a "$OUT"
    fi
  done

  if [ "$FAILED" -ne 0 ]; then
    echo "One or more packages failed – see output above." >&2
    exit 1
  fi
fi

echo ""
echo "────────────────────────────────────────────────────────────"
echo " Summary (averages across $COUNT run(s) per benchmark)"
echo "────────────────────────────────────────────────────────────"
echo ""

# ── Print summary ─────────────────────────────────────────────────────────────
go run "$ROOT/cmd/benchsummary/main.go" "$OUT"
