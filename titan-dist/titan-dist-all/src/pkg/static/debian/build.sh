#!/bin/bash

set -e
set -u

.  "`dirname $0`"/../pkgcommon/etc/config.sh
cd "`dirname $0`"/../

debian/generate-changes.sh
dpkg-buildpackage -k"$SIGNING_KEY_ID" \
	-Itarget -I.git -I.project -I.settings -I.classpath
