#!/bin/bash

# default command argument allows easily changing default behavior of container
if [ "$1" = 'janus-server' ]; then

    # copy the template
    cp /opt/janusgraph-template.properties /opt/janusgraph.properties

    # replace all instances of ${ENV_VARIABLE} with their value in the configuration files
    perl -pi.bak -e 's/\$\{([_A-Z]+)\}/$ENV{$1}/e' /opt/gremlin-server.yaml
    perl -pi.bak -e 's/\$\{([_A-Z]+)\}/$ENV{$1}/e' /opt/janusgraph.properties

    /opt/append-storage.sh
    /opt/append-index.sh

    exec /opt/janusgraph/bin/gremlin-server.sh /opt/gremlin-server.yaml
fi

exec "$@"
