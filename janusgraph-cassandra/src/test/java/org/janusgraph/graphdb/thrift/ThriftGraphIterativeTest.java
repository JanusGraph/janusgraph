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

package org.janusgraph.graphdb.thrift;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.cassandra.thrift.CassandraThriftStoreManager;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.graphdb.JanusGraphIterativeBenchmark;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.BeforeAll;

public class ThriftGraphIterativeTest extends JanusGraphIterativeBenchmark {

    @Override
    public WriteConfiguration getConfiguration() {
        return CassandraStorageSetup.getCassandraThriftGraphConfiguration(getClass().getSimpleName());
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        return new CassandraThriftStoreManager(new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,getConfiguration(), BasicConfiguration.Restriction.NONE));
    }


    @BeforeAll
    public static void beforeClass() {
        CassandraStorageSetup.startCleanEmbedded();
    }
}
