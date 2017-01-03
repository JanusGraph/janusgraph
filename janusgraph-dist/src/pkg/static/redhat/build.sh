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
[ ! -h "$TOPDIR/BUILD/janus-$RPM_VERSION" ] && ln -s "$REPO_ROOT" "$TOPDIR"/BUILD/janus-$RPM_VERSION

# Prepend version and release macro definitions to specfile
cd "`dirname $0`"
cat > janus.spec <<EOF
%define janus_version $RPM_VERSION
%define janus_release $RPM_RELEASE
EOF
cat janus.spec.base >> janus.spec

rpmbuild -bb janus.spec
rpmsign --addsign --key-id=$SIGNING_KEY_ID "$TOPDIR"/RPMS/noarch/janus*-$RPM_VERSION-$RPM_RELEASE.noarch.rpm
