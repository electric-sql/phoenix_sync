#!/usr/bin/env bash

./scripts/docs.sh

# MIX_DOCS_LIVE=true adds an automatic reload to the generated docs pages
if command -v fswatch >/dev/null 2>&1; then
  fswatch -0 -e "deps" -e "doc" -e "_build" . |
    xargs -0 -n 1 MIX_DOCS_LIVE=true ./scripts/docs.sh
elif command -v inotifywait >/dev/null 2>&1; then
  inotifywait -m -e modify -e create -e delete -r lib/ README.md |
    while read NEWFILE; do MIX_DOCS_LIVE=true ./scripts/docs.sh; done
else
  echo "no filesystem watch available: install fswatch or inotifywait"
  exit 1
fi
