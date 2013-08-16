#!/bin/bash

set -e
set -u

declare -r DN="`dirname $0`"
.  "$DN"/../pkgcommon/config.sh.in
cd "$DN"/../

dpkg-buildpackage -k"$SIGNING_KEY_ID" \
	-Itarget -I.git -I.project -I.settings -I.classpath
