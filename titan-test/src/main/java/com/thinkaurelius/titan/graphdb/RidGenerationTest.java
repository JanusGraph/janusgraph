package com.thinkaurelius.titan.graphdb;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;

public class RidGenerationTest {

    @Test
    public void testThreadBasedRidGeneration() throws InterruptedException {
        Configuration c = new BaseConfiguration();
        int n = 8;
        Preconditions.checkArgument(1 < n); // n <= 1 is useless
        Collection<byte[]> rids = new ConcurrentSkipListSet<byte[]>();
        RidThread[] threads = new RidThread[n];
        
        for (int i = 0; i < n; i++) {
            threads[i] = new RidThread(rids, c);
        }
        
        for (int i = 0; i < n; i++) {
            threads[i].start();
        }
        
        for (int i = 0; i < n; i++) {
            threads[i].join();
        }
        
        assertEquals(n, rids.size());
    }
    
    private static class RidThread extends Thread {
        
        private final Collection<byte[]> rids;
        private final Configuration c;
        
        private RidThread(Collection<byte[]> rids, Configuration c) {
            this.rids = rids;
            this.c = c;
        }
        
        @Override
        public void run() {
            rids.add(DistributedStoreManager.getRid(c));
        }
    };
}
