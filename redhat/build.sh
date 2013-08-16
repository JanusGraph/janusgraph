#!/bin/bash

set -e
set -u

TITAN_VERSION=0.4.0
TOPDIR=~/rpmbuild

cd "$(dirname $0)"/.. 
pwd
rsync --archive --delete --stats --exclude=.git \
	. "$TOPDIR"/BUILD/titan-$TITAN_VERSION
cd -
rpmbuild -bb titan.spec
