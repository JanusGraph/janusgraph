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

package org.janusgraph.diskstorage.cql;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.DistributedStoreManagerTest;
import org.janusgraph.diskstorage.common.DistributedStoreManager.Deployment;
import org.janusgraph.testcategory.OrderedKeyStoreTests;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

public class CQLDistributedStoreManagerTest extends DistributedStoreManagerTest<CQLStoreManager> {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Before
    public void setUp() throws BackendException {
        manager = new CQLStoreManager(CassandraStorageSetup.getCQLConfiguration(this.getClass().getSimpleName()));
        store = manager.openDatabase("distributedcf");
    }

    @After
    public void tearDown() throws BackendException {
        if (null != manager)
            manager.close();
    }

    @Override
    @Test
    @Category({ OrderedKeyStoreTests.class })
    public void testGetDeployment() {
        final Deployment deployment = CassandraStorageSetup.HOSTNAME == null ? Deployment.LOCAL : Deployment.REMOTE;
        assertEquals(deployment, manager.getDeployment());
    }
}
