#!/usr/bin/env bash
# Delete the co-located vectors.db (and its SQLite sidecars) and
# restart `msgvault build-embeddings` from scratch with verbose logging.
#
# Useful while debugging embedding pipelines: wipes any in-flight
# building generation, pending queue rows, and cached vectors so the
# next run starts a brand-new generation against the corpus.
#
# Usage:
#   scripts/reset-embeddings.sh [extra build-embeddings args]
#
# Env:
#   MSGVAULT_HOME   data dir (default: $HOME/.msgvault)
#   MSGVAULT_BIN    msgvault binary (default: ./msgvault, else `msgvault` on PATH)

set -euo pipefail

MSGVAULT_HOME="${MSGVAULT_HOME:-$HOME/.msgvault}"
VECTORS_DB="$MSGVAULT_HOME/vectors.db"

if [[ -n "${MSGVAULT_BIN:-}" ]]; then
  bin="$MSGVAULT_BIN"
elif [[ -x "./msgvault" ]]; then
  bin="./msgvault"
else
  bin="$(command -v msgvault || true)"
  if [[ -z "$bin" ]]; then
    echo "msgvault binary not found (set MSGVAULT_BIN, or build with 'make build')" >&2
    exit 1
  fi
fi

for f in "$VECTORS_DB" "$VECTORS_DB-wal" "$VECTORS_DB-shm" "$VECTORS_DB-journal"; do
  if [[ -e "$f" ]]; then
    echo "removing $f"
    rm -f "$f"
  fi
done

echo "running: $bin build-embeddings --full-rebuild --yes --verbose $*"
exec "$bin" build-embeddings --full-rebuild --yes --verbose "$@"
