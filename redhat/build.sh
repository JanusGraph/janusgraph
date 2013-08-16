#!/bin/bash

set -e
set -u

TITAN_VERSION=0.4.0
TOPDIR=~/rpmbuild

cd "`dirname $0`"/.. 
. pkgcommon/config.sh.in
pwd
rsync --archive --delete --stats --exclude=.git --exclude=target \
	. "$TOPDIR"/BUILD/titan-$TITAN_VERSION
cd "`dirname $0`"
rpmbuild -bb titan.spec
rpmsign --addsign --key-id=$SIGNING_KEY_ID "$TOPDIR"/RPMS/noarch/titan-*-"$TITAN_VERSION"*
