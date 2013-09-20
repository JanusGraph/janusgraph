#!/bin/bash

set -e
set -u

TITAN_VERSION=0.4.0
TOPDIR=~/rpmbuild

cd "`dirname $0`"/.. 
. pkgcommon/etc/config.sh
. pkgcommon/etc/version.sh
rsync --archive --delete --stats --exclude=.git --exclude=target \
	. "$TOPDIR"/BUILD/titan-$TITAN_VERSION
cd "`dirname $0`"

# Prepend version and release macro definitions to specfile
cat > titan.spec <<EOF
%define titan_version $RPM_VERSION
%define titan_release $RPM_RELEASE
EOF
cat titan.spec.base >> titan.spec

rpmbuild -bb titan.spec
rpmsign --addsign --key-id=$SIGNING_KEY_ID "$TOPDIR"/RPMS/noarch/titan*-$RPM_VERSION-$RPM_RELEASE.noarch.rpm
