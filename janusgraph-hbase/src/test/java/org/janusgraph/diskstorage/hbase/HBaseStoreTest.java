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

package org.janusgraph.diskstorage.hbase;

import org.janusgraph.HBaseContainer;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class HBaseStoreTest extends KeyColumnValueStoreTest {
    @Container
    public static final HBaseContainer hBaseContainer = new HBaseContainer();

    public HBaseStoreManager openStorageManager(ModifiableConfiguration config) throws BackendException {
        return new HBaseStoreManager(new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS, config.getConfiguration(), BasicConfiguration.Restriction.NONE));
    }

    public HBaseStoreManager openStorageManager() throws BackendException {
        return openStorageManager("");
    }

    public HBaseStoreManager openStorageManager(String tableName) throws BackendException {
        return openStorageManager(tableName, "");
    }

    public HBaseStoreManager openStorageManager(String tableName, String graphName) throws BackendException {
        return new HBaseStoreManager(hBaseContainer.getNamedConfiguration(tableName, graphName));
    }

    @Test
    public void testGetKeysWithKeyRange(TestInfo testInfo) throws Exception {
        super.testGetKeysWithKeyRange(testInfo);
    }

    @Override
    public HBaseStoreManager openStorageManagerForClearStorageTest() throws Exception {
        return openStorageManager(hBaseContainer.getModifiableConfiguration().set(GraphDatabaseConfiguration.DROP_ON_CLEAR, true));
    }

    @Test
    public void tableShouldEqualSuppliedTableName() throws BackendException {
        final HBaseStoreManager mgr = openStorageManager("randomTableName");
        assertEquals("randomTableName", mgr.getName());
    }

    @Test
    public void tableShouldEqualGraphName() throws BackendException {
        final HBaseStoreManager mgr = openStorageManager("", "randomGraphName");
        assertEquals("randomGraphName", mgr.getName());
    }
}
