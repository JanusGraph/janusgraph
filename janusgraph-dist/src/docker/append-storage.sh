#!/bin/bash

source /opt/append-util.sh

append_storage() {
    local KEY="storage.$1"
    local VAL=$2

    append_if_def ${KEY} ${VAL}
}

echo "# Storage Properties" >> /opt/janusgraph.properties

append_storage "batch-loading" $JANUS_STORAGE_BATCH_LOADING
append_storage "buffer-size" $JANUS_STORAGE_BUFFER_SIZE
append_storage "connection-timeout" $JANUS_STORAGE_CONNECTION_TIMEOUT
append_storage "directory" $JANUS_STORAGE_DIRECTORY
append_storage "hostname" $JANUS_STORAGE_HOSTNAME
append_storage "page-size" $JANUS_STORAGE_PAGE_SIZE
append_storage "paralell-backend-ops" $JANUS_STORAGE_PARALLEL_BACKEND_OPS
append_storage "password" $JANUS_STORAGE_PASSWORD
append_storage "port" $JANUS_STORAGE_PORT
append_storage "read-only" $JANUS_STORAGE_READ_ONLY
append_storage "read-time" $JANUS_STORAGE_READ_TIME
append_storage "setup-wait" $JANUS_STORAGE_SETUP_WAIT
append_storage "transactions" $JANUS_STORAGE_TRANSACTIONS
append_storage "username" $JANUS_STORAGE_USERNAME
append_storage "write-time" $JANUS_STORAGE_WRITE_TIME

# TODO: ADD BACKEND SPECIFIC PROPERTIES

