package org.janusgraph.diskstorage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.common.DistributedStoreManager.Deployment;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.testcategory.OrderedKeyStoreTests;

public abstract class DistributedStoreManagerTest<T extends DistributedStoreManager> {
    
    protected T manager;
    protected KeyColumnValueStore store;
    
    @Test
    @Category({ OrderedKeyStoreTests.class })
    public void testGetDeployment() {
        assertEquals(Deployment.LOCAL, manager.getDeployment());
    }
    
    @Test
    @Category({ OrderedKeyStoreTests.class })
    public void testGetLocalKeyPartition() throws BackendException {
        List<KeyRange> local = manager.getLocalKeyPartition();
        assertNotNull(local);
        assertEquals(1, local.size());
        assertNotNull(local.get(0).getStart());
        assertNotNull(local.get(0).getEnd());
    }
}
