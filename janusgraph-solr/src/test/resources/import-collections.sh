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

set -x

until $(curl --output /dev/null --silent --head --fail http://172.17.0.1:8983); do sleep 5; done
for core in $(cat /tmp/collections.txt); do /opt/solr/server/scripts/cloud-scripts/zkcli.sh -zkhost 172.17.0.1:9983 -cmd upconfig -confdir mydata -confname $core; done

echo "All collections imported"
