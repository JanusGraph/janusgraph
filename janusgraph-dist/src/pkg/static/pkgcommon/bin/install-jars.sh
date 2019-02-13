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

set -u
set -e

if [ -z "${1:-}" ]; then
    echo "Usage: $0 directory" >&2
    echo "  Copies JanusGraph's jars to the specified directory"
    echo "  The directory is created if it does not exist"
    exit 1
fi

. "`dirname $0`"/../etc/config.sh

[ ! -e "$1" ] && mkdir -p "$1"

cd "$1"
absolutepath="`pwd`"
cd - >/dev/null

cd "`dirname $0`"/../../janusgraph-dist

mvn dependency:copy-dependencies $MVN_OPTS \
    -Pjanusgraph-release \
    -DexcludeClassifiers=tests,test,javadoc \
    -DincludeScope=runtime \
    -DoutputDirectory="$absolutepath"
