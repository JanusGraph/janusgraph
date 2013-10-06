package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.HazelcastStorageSetup;

public class HazelcastCacheStoreBlueprintsTest extends AbstractHazelcastBlueprintsTest {
    public HazelcastCacheStoreBlueprintsTest() {
        super(HazelcastStorageSetup.getHazelcastCacheGraphConfig(true));
    }
}
