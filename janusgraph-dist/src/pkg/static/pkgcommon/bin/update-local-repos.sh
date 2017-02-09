#!/bin/bash

set -e
set -u

.  "`dirname $0`"/../etc/config.sh
.  "`dirname $0`"/../etc/version.sh
cd "`dirname $0`"/../../

echo 'Running reprepro...'
reprepro -b "$LOCAL_REPO_DEB_DIR" include $REPREPRO_CODENAME \
    ../janusgraph_"$DEB_VERSION"-"$DEB_RELEASE"_*.changes
echo 'Done running reprepro.'

[ ! -e "$LOCAL_REPO_RPM_DIR"/noarch/ ] && mkdir -p "$LOCAL_REPO_RPM_DIR"/noarch/
cp "$RPM_TOPDIR"/RPMS/noarch/janusgraph*-$RPM_VERSION-$RPM_RELEASE.noarch.rpm \
    "$LOCAL_REPO_RPM_DIR"/noarch/
echo 'Running createrepo...'
createrepo "$LOCAL_REPO_RPM_DIR"
echo 'Done running createrepo.'

cd "$LOCAL_REPO_DIR"
s3cmd --acl-public --delete-removed --verbose sync . "$S3_REPO_BUCKET"
