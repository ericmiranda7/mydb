#!/bin/bash

# Script to make HTTP requests 300 times
# Based on the requests in http-requests/SET.http

for i in {1..300}; do
  echo "Iteration $i of 300"

  # POST request to /set/loco
  curl -X POST http://localhost:8090/set/loco \
    -H "Content-Type: text/plain" \
    -d "caitanlakdkerfxkvladsf;kajsdf"

  # POST request to /set/jeff
  curl -X POST http://localhost:8090/set/jeff \
    -H "Content-Type: text/plain" \
    -d "latestjeff"

  # POST request to /set/arnold
  curl -X POST http://localhost:8090/set/arnold \
    -H "Content-Type: text/plain" \
    -d "latestarnold"

  echo ""
done

echo "Completed 300 iterations"

