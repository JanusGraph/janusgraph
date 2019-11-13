#!/bin/bash
#
# Copyright 2019 JanusGraph Authors
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

RUN_TESTS=
RUN_TINKERPOP_TESTS=
BUILD_IN_MEMORY=

function usage {
  echo -e "\nUsage: `basename $0` [OPTIONS]" \
          "\nBuild the current local JanusGraph project in a Docker container." \
          "\n\nOptions are:\n" \
          "\n\t-t, --tests              run standard test suite" \
          "\n\t-p, --tinkerpop-tests    run TinkerPop tests" \
          "\n\t-m, --in-memory          use tmpfs to build in-memory" \
          "\n\t-h, --help               show this message" \
          "\n"
}

while [ ! -z "$1" ]; do
  case "$1" in
    -t | --tests ) RUN_TESTS=true; shift ;;
    -p | --tinkerpop-tests ) RUN_TINKERPOP_TESTS=true; shift ;;
    -m | --in-memory ) BUILD_IN_MEMORY=true; shift ;;
    -h | --help ) usage; exit 0 ;;
    *) usage 1>&2; exit 1 ;;
  esac
done

JANUSGRAPH_BUILD_OPTIONS=""

[ -z "${RUN_TESTS}" ] && JANUSGRAPH_BUILD_OPTIONS="${JANUSGRAPH_BUILD_OPTIONS} -DskipTests"
[ -z "${RUN_TINKERPOP_TESTS}" ] || JANUSGRAPH_BUILD_OPTIONS="${JANUSGRAPH_BUILD_OPTIONS} -Dtest.skip.tp=false"

JANUSGRAPH_DOCKER_OPTIONS=""

[ -z "${BUILD_IN_MEMORY}" ] || JANUSGRAPH_DOCKER_OPTIONS="${JANUSGRAPH_DOCKER_OPTIONS} --tmpfs /usr/src/janusgraph_inmemory"

if [ "${RUN_TESTS}" ] || [ "${RUN_TINKERPOP_TESTS}" ]; then
    JANUSGRAPH_DOCKER_OPTIONS="-v /var/run/docker.sock:/var/run/docker.sock"
fi

docker build -t janusgraph-builder -f build.Dockerfile .
echo Starting container with: docker run ${JANUSGRAPH_DOCKER_OPTIONS} --rm -it janusgraph-builder ${JANUSGRAPH_BUILD_OPTIONS}
docker run ${JANUSGRAPH_DOCKER_OPTIONS} --rm -it janusgraph-builder ${JANUSGRAPH_BUILD_OPTIONS}
