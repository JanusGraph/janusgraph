#!/bin/bash

set -u
set -e

[ -n "${1:-}" ] || { echo "Usage: $0 directory-pattern" >&2; exit 1; }

. `dirname $0`/../config.sh.in

set -x

unset m
dir="`eval echo $1`"
[ ! -e "$dir" ] && mkdir -p "$dir"
rsync -qa "$PAYLOAD_DIR"/main/ "$dir"

for m in $MODULES; do
    dir="`eval echo $1`"
    mkdir -p "$dir"
    rsync -qa "$PAYLOAD_DIR/$m/" "$dir"
done
