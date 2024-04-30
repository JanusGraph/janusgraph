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

JANUS_PROPS="${JANUS_CONFIG_DIR}/janusgraph.properties"
JANUSGRAPH_SERVER_YAML="${JANUS_CONFIG_DIR}/janusgraph-server.yaml"

# running as root; step down to run as janusgraph user
if [ "$1" == 'janusgraph' ] && [ "$(id -u)" == "0" ]; then
  mkdir -p ${JANUS_DATA_DIR} ${JANUS_CONFIG_DIR}
  chown janusgraph:janusgraph ${JANUS_DATA_DIR} ${JANUS_CONFIG_DIR}
  chmod 700 ${JANUS_DATA_DIR} ${JANUS_CONFIG_DIR}

  exec chroot --skip-chdir --userspec janusgraph:janusgraph / "${BASH_SOURCE}" "$@"
fi

# running as non root user
if [ "$1" == 'janusgraph' ]; then
  # setup config directory
  mkdir -p ${JANUS_DATA_DIR} ${JANUS_CONFIG_DIR}
  cp conf/janusgraph-${JANUS_PROPS_TEMPLATE}-server.properties ${JANUS_PROPS}
  cp conf/janusgraph-server.yaml ${JANUSGRAPH_SERVER_YAML}
  chown "$(id -u):$(id -g)" ${JANUS_DATA_DIR} ${JANUS_CONFIG_DIR}
  chmod 700 ${JANUS_DATA_DIR} ${JANUS_CONFIG_DIR}
  chmod -R 600 ${JANUS_CONFIG_DIR}/*

  # apply configuration from environment
  while IFS=' ' read -r envvar_key envvar_val; do
    if [[ "${envvar_key}" =~ janusgraph\. ]] && [[ ! -z ${envvar_val} ]]; then
      # strip namespace and use properties file delimiter for janusgraph properties
      envvar_key=${envvar_key#"janusgraph."}
      # Add new or update existing field in configuration file
      if grep -q -E "^\s*${envvar_key}\s*=\.*" ${JANUS_PROPS}; then
        sed -ri "s#^(\s*${envvar_key}\s*=).*#\\1${envvar_val}#" ${JANUS_PROPS}
      else
        echo "${envvar_key}=${envvar_val}" >> ${JANUS_PROPS}
      fi
    elif [[ "${envvar_key}" =~ gremlinserver(%d)?[.]{1}(.+) ]]; then
      # Check for edit mode %d after prefix
      if [[ ${BASH_REMATCH[1]} == "%d" ]]; then edit_mode="d"; else edit_mode="w"; fi
      # strip namespace from env variable and get value
      envvar_key=${BASH_REMATCH[2]}
      if [[ edit_mode == "d" ]]; then envvar_val=""; fi
      # add new or update existing field in configuration file
      yq ${edit_mode} -P -i ${JANUSGRAPH_SERVER_YAML} ${envvar_key} ${envvar_val}
    else
      continue
    fi
  done < <(env | sort -r | awk -F= '{ st = index($0, "="); print $1 " " substr($0, st+1) }')

  if [ "$2" == 'show-config' ]; then
    echo "# contents of ${JANUS_PROPS}"
    cat "$JANUS_PROPS"
    echo "---------------------------------------"
    echo "# contents of ${JANUSGRAPH_SERVER_YAML}"
    cat "$JANUSGRAPH_SERVER_YAML"
    exit 0
  else
    # wait for storage
    if ! [ -z "${JANUS_STORAGE_TIMEOUT:-}" ]; then
      F="$(mktemp --suffix .groovy)"
      echo "graph = JanusGraphFactory.open('${JANUS_PROPS}')" > $F
      timeout "${JANUS_STORAGE_TIMEOUT}s" bash -c \
        "until bin/gremlin.sh -e $F > /dev/null 2>&1; do echo \"waiting for storage...\"; sleep 5; done"
      rm -f "$F"
    fi

    /usr/local/bin/load-initdb.sh &

    exec ${JANUS_HOME}/bin/janusgraph-server.sh ${JANUSGRAPH_SERVER_YAML}
  fi
fi

# override hosts for remote connections with Gremlin Console
if ! [ -z "${GREMLIN_REMOTE_HOSTS:-}" ]; then
  sed -i "s/hosts\s*:.*/hosts: [$GREMLIN_REMOTE_HOSTS]/" ${JANUS_HOME}/conf/remote.yaml
  sed -i "s/hosts\s*:.*/hosts: [$GREMLIN_REMOTE_HOSTS]/" ${JANUS_HOME}/conf/remote-objects.yaml
fi

exec "$@"
