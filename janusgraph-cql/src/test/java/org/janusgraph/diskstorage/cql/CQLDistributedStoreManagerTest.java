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

import org.janusgraph.JanusGraphCassandraContainer;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.DistributedStoreManagerTest;
import org.janusgraph.diskstorage.common.DistributedStoreManager.Deployment;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.testutil.FeatureFlag;
import org.janusgraph.testutil.JanusGraphFeature;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class CQLDistributedStoreManagerTest extends DistributedStoreManagerTest<CQLStoreManager> {

    @Container
    public static final JanusGraphCassandraContainer cqlContainer = new JanusGraphCassandraContainer();

    protected ModifiableConfiguration getBaseStorageConfiguration() {
        return cqlContainer.getConfiguration(getClass().getSimpleName());
    }

    @BeforeEach
    public void setUp() throws BackendException {
        manager = new CachingCQLStoreManager(getBaseStorageConfiguration());
        store = manager.openDatabase("distributedcf");
    }

    @AfterEach
    public void tearDown() throws BackendException {
        if (null != manager)
            manager.close();
    }

    @Override
    @Test
    @FeatureFlag(feature = JanusGraphFeature.OrderedScan)
    @Disabled(value = "Test can't run inside of a container")
    public void testGetDeployment() {
        final Deployment deployment = Deployment.LOCAL;
        assertEquals(deployment, manager.getDeployment());
    }
}
