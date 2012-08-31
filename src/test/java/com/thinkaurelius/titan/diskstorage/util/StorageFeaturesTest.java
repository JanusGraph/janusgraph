package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class StorageFeaturesTest {
    
    @Test
    public void testFeaturesImplementation() {
        StorageFeaturesImplementation f1 = new StorageFeaturesImplementation(ImmutableMap.of("supportsScan",false,"isTransactional",true));
        assertNotNull(f1);
        assertFalse(f1.supportsScan());
        assertTrue(f1.isTransactional());
        try {
            f1 = new StorageFeaturesImplementation(ImmutableMap.of("supportsScan",true,"something",false));
            fail();
        } catch (IllegalArgumentException e) {}
    }
    
    
}
