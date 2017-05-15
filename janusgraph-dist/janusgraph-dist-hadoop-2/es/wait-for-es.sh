#!/bin/bash

set -e

host="$1"

shift
cmd="$@"

until $(curl --output /dev/null --silent --head --fail http://$host:9200); do
  printf '.'
  sleep 5
done

>&2 echo "Elasticsearch is up"

exec $cmd

