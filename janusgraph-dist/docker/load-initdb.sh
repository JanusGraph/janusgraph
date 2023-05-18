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

# exit early if directory is empty
if ! [ "$(ls -A ${JANUS_INITDB_DIR})" ]; then
  exit 0
fi

# wait for JanusGraph Server
if ! [ -z "${JANUS_SERVER_TIMEOUT:-}" ]; then
  timeout "${JANUS_SERVER_TIMEOUT}s" bash -c \
  "until true &>/dev/null </dev/tcp/127.0.0.1/8182; do echo \"waiting for JanusGraph Server...\"; sleep 5; done"
fi

for f in ${JANUS_INITDB_DIR}/*; do
  case "$f" in
    *.groovy) echo "$0: running $f"; ${JANUS_HOME}/bin/gremlin.sh -e "$f"; echo ;;
    *)        echo "$0: ignoring $f" ;;
  esac
  echo
done
