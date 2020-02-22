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

package org.janusgraph.diskstorage.cassandra.astyanax;

import org.janusgraph.JanusGraphCassandraThriftContainer;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreTest;
import org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class AstyanaxStoreTest extends AbstractCassandraStoreTest {
    @Container
    public static final JanusGraphCassandraThriftContainer thriftContainer = new JanusGraphCassandraThriftContainer();

    @Override
    public ModifiableConfiguration getBaseStorageConfiguration(String keyspace) {
        return thriftContainer.getAstyanaxConfiguration(keyspace);
    }

    @Override
    public ModifiableConfiguration getBaseStorageConfiguration() {
        return getBaseStorageConfiguration(getClass().getSimpleName());
    }

    @Override
    public AbstractCassandraStoreManager openStorageManager(Configuration c) throws BackendException {
        return new AstyanaxStoreManager(c);
    }
}
