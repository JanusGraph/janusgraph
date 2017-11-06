#!/bin/bash

append_if_def() {

# $1 is key $2 is value
if [ -n "${2}" ]; then
echo "${1}=${2}" >> /opt/janusgraph.properties
fi

}
