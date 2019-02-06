// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.pkgtest;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.janusgraph.CassandraStorageSetup;

public class CassandraThriftAssemblyIT extends AbstractJanusGraphAssemblyIT {

    @BeforeAll
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Test
    public void testCassandraThriftSimpleSession() throws Exception {
        testSimpleGremlinSession("conf/janusgraph-cassandra.properties", "cassandrathrift");
    }
}
