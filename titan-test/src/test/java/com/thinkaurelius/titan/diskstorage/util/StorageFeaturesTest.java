package com.thinkaurelius.titan.diskstorage.util;

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
        try {
            features = new StoreFeatures();
            features.supportsBatchMutation();
            fail();
        } catch (AssertionError e) {
        }

        try {
            features = new StoreFeatures();
            features.hasLocalKeyPartition = true;
            features.hasLocalKeyPartition();
            fail();
        } catch (AssertionError e) {
        }
        features = new StoreFeatures();
        features.supportsOrderedScan = false;
        features.supportsUnorderedScan = false;
        features.supportsBatchMutation = true;
        features.supportsTransactions = false;
        features.supportsMultiQuery = false;
        features.supportsConsistentKeyOperations = true;
        features.supportsLocking = false;
        features.isKeyOrdered = false;
        features.isDistributed = true;
        features.hasLocalKeyPartition = false;
        assertNotNull(features);
        assertFalse(features.supportsScan());
        assertFalse(features.supportsTransactions());
        assertTrue(features.isDistributed());
    }


}
