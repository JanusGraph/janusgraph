package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class StorageFeaturesTest {
    
    @Test
    public void testFeaturesImplementation() {
        StoreFeatures f1 = new StoreFeatures(ImmutableMap.of("supportsScan",false,"isTransactional",true));
        assertNotNull(f1);
        assertFalse(f1.supportsScan());
        assertTrue(f1.isTransactional());
        try {
            f1 = new StoreFeatures(ImmutableMap.of("supportsScan",true,"something",false));
            fail();
        } catch (IllegalArgumentException e) {}
    }
    
    
}
