package com.thinkaurelius.titan.graphdb.idmanagement;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.IDAuthority;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;
import com.thinkaurelius.titan.graphdb.database.idassigner.StandardIDPool;
import com.thinkaurelius.titan.util.datastructures.IntHashSet;
import com.thinkaurelius.titan.util.datastructures.IntSet;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class IDPoolTest {

    @Test
    public void testStandardIDPool() {
        int noIds = 2000000;
        MockIDAuthority idauth = new MockIDAuthority(200,0);
        StandardIDPool pool = new StandardIDPool(idauth,0);
        IntSet ids = new IntHashSet(noIds);
        for (int i=0;i<noIds;i++) {
            long id = pool.nextID();
            assertTrue(id<Integer.MAX_VALUE);
            assertFalse(ids.contains((int)id));
            ids.add((int)id);
        }
    }

    static class MockIDAuthority implements IDAuthority {
        
        private int idcounter = 1;
        private final int incrementBy;
        private final int partitionID;

        public MockIDAuthority() {
            this(200,0);
        }

        public MockIDAuthority(int increment, int partitionID) {
            this.incrementBy=increment;
            this.partitionID=partitionID;
        }
        
        
        @Override
        public synchronized long[] getIDBlock(int partition) throws StorageException {
            Preconditions.checkArgument(partition==partitionID);
            int lower = idcounter;
            idcounter += incrementBy;
            int upper = idcounter;
            return new long[]{lower,upper};
        }

        @Override
        public void setIDBlockSizer(IDBlockSizer sizer) {
            throw new UnsupportedOperationException();
        }
    }

}
