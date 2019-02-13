#!/bin/bash
# Copyright 2019 JanusGraph Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License

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
