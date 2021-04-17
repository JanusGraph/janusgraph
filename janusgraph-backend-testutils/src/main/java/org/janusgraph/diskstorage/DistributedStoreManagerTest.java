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

package org.janusgraph.diskstorage;

import org.janusgraph.JanusGraphBaseStoreFeaturesTest;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.testutil.FeatureFlag;
import org.janusgraph.testutil.JanusGraphFeature;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public abstract class DistributedStoreManagerTest<T extends DistributedStoreManager> implements JanusGraphBaseStoreFeaturesTest {

    protected T manager;
    protected KeyColumnValueStore store;

    public StoreFeatures getStoreFeatures(){
        return manager.getFeatures();
    }

    @Test
    @FeatureFlag(feature = JanusGraphFeature.OrderedScan)
    public void testGetLocalKeyPartition() throws BackendException {
        assumeTrue(manager.getFeatures().hasLocalKeyPartition());
        List<KeyRange> local = manager.getLocalKeyPartition();
        assertNotNull(local);
        assertEquals(1, local.size());
        assertNotNull(local.get(0).getStart());
        assertNotNull(local.get(0).getEnd());
    }
}
