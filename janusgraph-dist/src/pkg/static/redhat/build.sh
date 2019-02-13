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
