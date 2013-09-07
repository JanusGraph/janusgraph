# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# This script is derived from the bin/cassandra.in.sh script shipped
# with Apache Cassandra version 1.2.8.
#

TITAN_HOME="/usr/share/titan"
TITAN_CONF="/etc/titan"

TITAN_ENV_DIR="$TITAN_CONF/env.d"
TITAN_CFG="$TITAN_CONF/config.properties"
REXSTER_CFG="$TITAN_CONF/rexster.xml"

# JAVA_HOME can optionally be set here
#JAVA_HOME=/usr/local/jdk6

# The java classpath (required)
CLASSPATH="$TITAN_CONF"

# Include Titan core jars
for jar in "$TITAN_HOME"/lib/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done
