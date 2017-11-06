#!/bin/bash

source /opt/append-util.sh

append_vars() {

local INDEX_DEFN="$(echo ${1} | tr '[A-Z]' '[a-z]')"
local BASE="index.${INDEX_DEFN}"

cat <<EOT >> /opt/janusgraph.properties
# Index Definition For ${1}
${BASE}.backend=${JANUS_INDEX_BACKEND:-elasticsearch}
${BASE}.hostname=${JANUS_INDEX_HOSTNAME:-elastic-host}
${BASE}.index-name=${JANUS_INDEX_NAME:-janusgraph}
${BASE}.map-name=${JANUS_INDEX_MAP_NAME:-true}
${BASE}.max-result-set-size=${JANUS_INDEX_MAX_RESULT_SET:-100000}
EOT

append_if_def "${BASE}.port" "${JANUS_INDEX_PORT}"

if [ "elasticsearch" = "${JANUS_INDEX_BACKEND}" ]; then

BASE_ES="${BASE}.elasticsearch"

cat <<EOT >> /opt/janusgraph.properties
${BASE_ES}.client-only=${JANUS_INDEX_ES_CLIENT_ONLY:-true}
${BASE_ES}.cluster-name=${JANUS_INDEX_ES_CLUSTER_NAME:-elasticsearch}
${BASE_ES}.health-request-timeout=${JANUS_INDEX_ES_HEALTH_TIMEOUT:-30s}
${BASE_ES}.ignore-cluster-name=${JANUS_INDEX_ES_IGNORE_CLUSTER_NAME:-true}
${BASE_ES}.interface=${JANUS_INDEX_ES_INTERFACE:-TRANSPORT_CLIENT}
${BASE_ES}.load-default-node-settings=${JANUS_INDEX_ES_LOAD_DEFAULT_NODE_SETTINGS:-true}
${BASE_ES}.local-mode=${JANUS_INDEX_ES_LOCAL_MODE:-false}
${BASE_ES}.sniff=${JANUS_INDEX_ES_SNIFF:-true}
${BASE_ES}.ttl-interval=${JANUS_INDEX_ES_TTL_INTERVAL:-5s}
${BASE_ES}.create.sleep=${JANUS_INDEX_ES_CREATE_SLEEP:-200}
EOT

fi

}

echo ${JANUS_INDEX_DEFINED_NAME}

# check if an index is defined
if [ -z "${JANUS_INDEX_DEFINED_NAME}" ]; then
    exit 0;
fi

append_vars ${JANUS_INDEX_DEFINED_NAME}
