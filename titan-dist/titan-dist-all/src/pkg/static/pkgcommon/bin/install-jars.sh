#!/bin/bash

set -u
set -e

[ -n "${1:-}" ] || { echo "Usage: $0 directory-pattern" >&2; exit 1; }

. `dirname $0`/../etc/config.sh

unset m
dir="`eval echo $1`"
[ ! -e "$dir" ] && mkdir -p "$dir"
for j in `cat $DEP_DIR/jar-main.txt`; do
    cp "$j" "$dir"
done

for m in $MODULES; do
    dir="`eval echo $1`"
    mkdir -p "$dir"
    for j in `cat $DEP_DIR/jar-$m.txt`; do
        cp "$j" "$dir"
    done
done
