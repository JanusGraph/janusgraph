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

# Returns the absolute path of this script regardless of symlinks
abs_path() {
    # From: https://stackoverflow.com/a/246128
    #   - To resolve finding the directory after symlinks
    SOURCE="${BASH_SOURCE[0]}"
    while [ -h "$SOURCE" ]; do
        DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    echo "$( cd -P "$( dirname "$SOURCE" )" && pwd )"
}

CASSANDRA_HOME=$(cd `abs_path`/.. && pwd)

# The directory where Cassandra's configs live (required)
CASSANDRA_CONF=$CASSANDRA_HOME/conf/cassandra

# This can be the path to a jar file, or a directory containing the 
# compiled classes. NOTE: This isn't needed by the startup script,
# it's just used here in constructing the classpath.
cassandra_bin="$CASSANDRA_HOME/build/classes/main"
cassandra_bin="$cassandra_bin:$CASSANDRA_HOME/build/classes/thrift"
#cassandra_bin="$cassandra_home/build/cassandra.jar"

# the default location for commitlogs, sstables, and saved caches
# if not set in cassandra.yaml
cassandra_storagedir="$CASSANDRA_HOME/data"

# JAVA_HOME can optionally be set here
#JAVA_HOME=/usr/local/jdk6

# The java classpath (required)
CLASSPATH="$CASSANDRA_CONF:$cassandra_bin"

CLASSPATH="$CLASSPATH":$(ls "$CASSANDRA_HOME"/lib/slf4j-log4j12-*.jar)

for jar in "$CASSANDRA_HOME"/lib/*.jar; do
    if [ $jar != slf4j-log4j12* ] ; then
        CLASSPATH="$CLASSPATH:$jar"
    fi
done

# This system property is referenced in log4j-server.properties
logdir="$CASSANDRA_HOME/logs"

# Special-case path variables.
case "`uname`" in
    CYGWIN*) logdir="`echo $logdir | cygpath --windows --path -f -`" ;;
esac

export JVM_OPTS="$JVM_OPTS -Djanusgraph.logdir=$logdir"

# Change to $CASSANDRA_HOME
# (typically the directory containing bin/)
cd "$CASSANDRA_HOME"
