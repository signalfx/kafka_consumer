#!/bin/bash
set -x
set -e
set -o pipefail

# Make sure all vendor/test libs are installed so that vet works properly
(cd vendor && find . -type d -exec bash -c '(cd "$0" && if [ -n "$(ls -A *.go 2>/dev/null)" ]; then go install; fi)' {} \;)

x=$(gobuild list)
for y in $x; do
  gobuild check "$y" 2>&1
done

rm -f kafka_consumer
CGO_ENABLED=0 go build -v -installsuffix . -ldflags="-s -w"
file kafka_consumer | grep "statically linked"
ls -lad kafka_consumer
