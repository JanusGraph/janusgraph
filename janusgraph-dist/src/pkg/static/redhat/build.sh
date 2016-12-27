#!/bin/bash

set -e
set -u

TOPDIR=~/rpmbuild
cd "`dirname $0`/.."
REPO_ROOT="`pwd`"
cd - >/dev/null

cd "`dirname $0`"/.. 
. pkgcommon/etc/config.sh
. pkgcommon/etc/version.sh
[ ! -h "$TOPDIR/BUILD/titan-$RPM_VERSION" ] && ln -s "$REPO_ROOT" "$TOPDIR"/BUILD/titan-$RPM_VERSION

# Prepend version and release macro definitions to specfile
cd "`dirname $0`"
cat > titan.spec <<EOF
%define titan_version $RPM_VERSION
%define titan_release $RPM_RELEASE
EOF
cat titan.spec.base >> titan.spec

rpmbuild -bb titan.spec
rpmsign --addsign --key-id=$SIGNING_KEY_ID "$TOPDIR"/RPMS/noarch/titan*-$RPM_VERSION-$RPM_RELEASE.noarch.rpm
