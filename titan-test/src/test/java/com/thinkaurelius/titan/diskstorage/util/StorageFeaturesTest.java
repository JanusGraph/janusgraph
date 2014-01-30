package com.thinkaurelius.titan.diskstorage.util;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StandardStoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StorageFeaturesTest {

    @Test
    public void testFeaturesImplementation() {
        StoreFeatures features;

        features = new StandardStoreFeatures.Builder().build();

        assertFalse(features.hasBatchMutation());
        assertFalse(features.hasLocking());
        assertFalse(features.isDistributed());
        assertFalse(features.hasScan());

        features = new StandardStoreFeatures.Builder().locking(true).build();

        assertFalse(features.hasBatchMutation());
        assertTrue(features.hasLocking());
        assertFalse(features.isDistributed());

        features = new StandardStoreFeatures.Builder().batchMutation(true).unorderedScan(true).build();

        assertTrue(features.hasBatchMutation());
        assertTrue(features.hasUnorderedScan());
        assertFalse(features.hasOrderedScan());
        assertTrue(features.hasScan());
        assertFalse(features.isDistributed());
        assertFalse(features.hasLocking());
    }


}
