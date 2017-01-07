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
[ ! -h "$TOPDIR/BUILD/janusgraph-$RPM_VERSION" ] && ln -s "$REPO_ROOT" "$TOPDIR"/BUILD/janusgraph-$RPM_VERSION

# Prepend version and release macro definitions to specfile
cd "`dirname $0`"
cat > janusgraph.spec <<EOF
%define janusgraph_version $RPM_VERSION
%define janusgraph_release $RPM_RELEASE
EOF
cat janusgraph.spec.base >> janusgraph.spec

rpmbuild -bb janusgraph.spec
rpmsign --addsign --key-id=$SIGNING_KEY_ID "$TOPDIR"/RPMS/noarch/janusgraph*-$RPM_VERSION-$RPM_RELEASE.noarch.rpm
