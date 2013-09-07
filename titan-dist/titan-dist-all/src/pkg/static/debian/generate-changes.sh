#!/bin/bash

set -u
set -e

. pkgcommon/etc/config.sh
. pkgcommon/etc/version.sh

cat > debian/changelog <<EOF
titan ($DEB_VERSION-$DEB_RELEASE) unstable; urgency=low

  * New release

 -- Dan LaRocque <dan@thinkaurelius.com>  `date +'%a, %d %b %Y %H:%M:%S %z'`

EOF
cat >>debian/changelog <debian/changelog.past