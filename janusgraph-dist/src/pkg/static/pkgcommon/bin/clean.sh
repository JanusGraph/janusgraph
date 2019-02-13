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

# Change to janusgraph repository root
cd "`dirname $0`/../../"

# Load config
. pkgcommon/etc/config.sh

# Clean dependency dir
if [ -e "$DEP_DIR" ]; then
    echo Deleting contents of "$DEP_DIR"...
    find $DEP_DIR -mindepth 1 -maxdepth 1 -exec rm -rf '{}' \;
fi
