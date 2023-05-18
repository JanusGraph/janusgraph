#!/bin/bash
#
# Copyright 2023 JanusGraph Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

IMAGE=$1

if [ -z "${IMAGE:-}" ]; then
  echo "usage: test-image.sh image-name"
  exit 1
fi

echo "Testing ${IMAGE}..."

ID=$(docker run --rm -d \
  --health-cmd='./bin/gremlin.sh -e scripts/remote-connect.groovy' \
  --health-interval=10s \
  --health-retries=5 \
  --health-start-period=10s \
  ${IMAGE})

for i in $(seq 1 10); do
  res=$(docker ps --filter "id=$ID" --filter "health=healthy" -q)
  if [ -z "${res:-}" ]; then
    status=1
    sleep 10
    continue
  fi
  status=0
  break
done

if [ -z "${res:-}" ]; then
  echo "Timeout waiting for health check:"
  res=$(docker inspect --format "{{json .State.Health }}" $ID)
  # pretty print last output with jq if available
  echo $res | jq --raw-output '.[-1].Output' || echo $res
fi

docker stop $ID

exit $status
