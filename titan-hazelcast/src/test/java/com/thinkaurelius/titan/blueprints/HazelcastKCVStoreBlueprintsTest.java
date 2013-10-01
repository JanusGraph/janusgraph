package com.thinkaurelius.titan.blueprints;


import com.thinkaurelius.titan.HazelcastStorageSetup;

public class HazelcastKCVStoreBlueprintsTest extends AbstractHazelcastBlueprintsTest {
    public HazelcastKCVStoreBlueprintsTest() {
        super(HazelcastStorageSetup.getHazelcastKCVSGraphConfig(true));
    }
}