package com.thinkaurelius.titan.graphdb.idmanagement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.easymock.EasyMock.*;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.IDAuthority;
import com.thinkaurelius.titan.diskstorage.IDBlock;
import com.thinkaurelius.titan.diskstorage.TemporaryBackendException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;
import junit.framework.Assert;
import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.Test;

import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDPoolExhaustedException;
import com.thinkaurelius.titan.graphdb.database.idassigner.StandardIDPool;
import com.thinkaurelius.titan.util.datastructures.IntHashSet;
import com.thinkaurelius.titan.util.datastructures.IntSet;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IDPoolTest {



    @Test
    public void testStandardIDPool1() throws InterruptedException {
        final MockIDAuthority idauth = new MockIDAuthority(200);
        testIDPoolWith(new IDPoolFactory() {
            @Override
            public StandardIDPool get(int partitionID) {
                return new StandardIDPool(idauth, partitionID, partitionID, Integer.MAX_VALUE, new StandardDuration(2000L, TimeUnit.MILLISECONDS), 0.2);
            }
        }, 1000, 6, 100000);
    }

    @Test
    public void testStandardIDPool2() throws InterruptedException {
        final MockIDAuthority idauth = new MockIDAuthority(10000, Integer.MAX_VALUE, 2000);
        testIDPoolWith(new IDPoolFactory() {
            @Override
            public StandardIDPool get(int partitionID) {
                return new StandardIDPool(idauth, partitionID, partitionID, Integer.MAX_VALUE, new StandardDuration(4000, TimeUnit.MILLISECONDS), 0.1);
            }
        }, 2, 5, 10000);
    }

    @Test
    public void testStandardIDPool3() throws InterruptedException {
        final MockIDAuthority idauth = new MockIDAuthority(200);
        testIDPoolWith(new IDPoolFactory() {
            @Override
            public StandardIDPool get(int partitionID) {
                return new StandardIDPool(idauth, partitionID, partitionID, Integer.MAX_VALUE, new StandardDuration(2000, TimeUnit.MILLISECONDS), 0.2);
            }
        }, 10, 20, 100000);
    }

    private void testIDPoolWith(IDPoolFactory poolFactory, final int numPartitions,
                                       final int numThreads, final int attemptsPerThread) throws InterruptedException {
        final Random random = new Random();
        final IntSet[] ids = new IntSet[numPartitions];
        final StandardIDPool[] idPools = new StandardIDPool[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            ids[i] = new IntHashSet(attemptsPerThread * numThreads / numPartitions);
            int partition = i*100;
            idPools[i] = poolFactory.get(partition);
        }

        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int attempt = 0; attempt < attemptsPerThread; attempt++) {
                        int offset = random.nextInt(numPartitions);
                        long id = idPools[offset].nextID();
                        assertTrue(id < Integer.MAX_VALUE);
                        IntSet idset = ids[offset];
                        synchronized (idset) {
                            assertFalse(idset.contains((int) id));
                            idset.add((int) id);
                        }
                    }
                }
            });
            threads[i].start();
        }
        for (int i = 0; i < numThreads; i++) threads[i].join();
        for (int i = 0; i < idPools.length; i++) idPools[i].close();
        //Verify consecutive id assignment
        for (int i = 0; i < ids.length; i++) {
            IntSet set = ids[i];
            int max = 0;
            int[] all = set.getAll();
            for (int j=0;j<all.length;j++) if (all[j]>max) max=all[j];
            for (int j=1;j<=max;j++) assertTrue(i+ " contains: " + j,set.contains(j));
        }
    }

    @Test
    public void testAllocationTimeout() {
        final MockIDAuthority idauth = new MockIDAuthority(10000, Integer.MAX_VALUE, 5000);
        StandardIDPool pool = new StandardIDPool(idauth, 1, 1, Integer.MAX_VALUE, new StandardDuration(4000, TimeUnit.MILLISECONDS), 0.1);
        try {
            pool.nextID();
            fail();
        } catch (TitanException e) {

        }

    }

    @Test
    public void testAllocationTimeoutAndRecovery() throws BackendException {
        IMocksControl ctrl = EasyMock.createStrictControl();

        final int partition = 42;
        final int idNamespace = 777;
        final Duration timeout = new StandardDuration(1L, TimeUnit.SECONDS);

        final IDAuthority mockAuthority = ctrl.createMock(IDAuthority.class);

        // Sleep for two seconds, then throw a backendexception
        // this whole delegate could be deleted if we abstracted StandardIDPool's internal executor and stopwatches
        expect(mockAuthority.getIDBlock(partition, idNamespace, timeout)).andDelegateTo(new IDAuthority() {
            @Override
            public IDBlock getIDBlock(int partition, int idNamespace, Duration timeout) throws BackendException {
                try {
                    Thread.sleep(2000L);
                } catch (InterruptedException e) {
                    fail();
                }
                throw new TemporaryBackendException("slow backend");
            }

            @Override
            public List<KeyRange> getLocalIDPartition() throws BackendException {
                throw new IllegalArgumentException();
            }

            @Override
            public void setIDBlockSizer(IDBlockSizer sizer) {
                throw new IllegalArgumentException();
            }

            @Override
            public void close() throws BackendException {
                throw new IllegalArgumentException();
            }

            @Override
            public String getUniqueID() {
                throw new IllegalArgumentException();
            }
        });
        expect(mockAuthority.getIDBlock(partition, idNamespace, timeout)).andReturn(new IDBlock() {
            @Override
            public long numIds() {
                return 2;
            }

            @Override
            public long getId(long index) {
                return 200;
            }
        });

        ctrl.replay();
        StandardIDPool pool = new StandardIDPool(mockAuthority, partition, idNamespace, Integer.MAX_VALUE, timeout, 0.1);
        try {
            pool.nextID();
            fail();
        } catch (TitanException e) {

        }

        long nextID = pool.nextID();
        assertEquals(200, nextID);

        ctrl.verify();
    }

    @Test
    public void testPoolExhaustion1() {
        MockIDAuthority idauth = new MockIDAuthority(200);
        int idUpper = 10000;
        StandardIDPool pool = new StandardIDPool(idauth, 0, 1, idUpper, new StandardDuration(2000, TimeUnit.MILLISECONDS), 0.2);
        for (int i = 1; i < idUpper * 2; i++) {
            try {
                long id = pool.nextID();
                assertTrue(id < idUpper);
            } catch (IDPoolExhaustedException e) {
                assertEquals(idUpper, i);
                break;
            }
        }
    }

    @Test
    public void testPoolExhaustion2() {
        int idUpper = 10000;
        MockIDAuthority idauth = new MockIDAuthority(200, idUpper);
        StandardIDPool pool = new StandardIDPool(idauth, 0, 1, Integer.MAX_VALUE, new StandardDuration(2000, TimeUnit.MILLISECONDS), 0.2);
        for (int i = 1; i < idUpper * 2; i++) {
            try {
                long id = pool.nextID();
                assertTrue(id < idUpper);
            } catch (IDPoolExhaustedException e) {
                assertEquals(idUpper, i);
                break;
            }
        }
    }

    interface IDPoolFactory {
        public StandardIDPool get(int partitionID);
    }

}
