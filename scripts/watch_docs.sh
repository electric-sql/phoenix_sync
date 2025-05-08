./scripts/docs.sh

fswatch -0 -e "deps" -e "doc" -e "_build" . | xargs -0 -n 1 ./scripts/docs.sh
