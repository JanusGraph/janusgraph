#!/bin/bash

set -u
set -e

if [ -z "${1:-}" ]; then
    echo "Usage: $0 directory" >&2
    echo "  Copies JanusGraph's OS-agnostic files to the specified root"
    echo "  The directory is created if it does not exist"
    exit 1
fi

. "`dirname $0`"/../etc/config.sh

[ ! -e "$1" ] && mkdir -p "$1"

rsync -qa "$PAYLOAD_DIR"/ "$1"
