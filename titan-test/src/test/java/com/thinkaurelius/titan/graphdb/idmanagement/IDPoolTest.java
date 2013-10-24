package com.thinkaurelius.titan.graphdb.idmanagement;

import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.idmanagement.ConsistentKeyIDManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory.InMemoryKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDPoolExhaustedException;
import com.thinkaurelius.titan.graphdb.database.idassigner.StandardIDPool;
import com.thinkaurelius.titan.util.datastructures.IntHashSet;
import com.thinkaurelius.titan.util.datastructures.IntSet;
import org.apache.commons.configuration.BaseConfiguration;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IDPoolTest {



    @Test
    public void testStandardIDPool1() throws InterruptedException {
        final MockIDAuthority idauth = new MockIDAuthority(200);
        testIDPoolWith(new IDPoolFactory() {
            @Override
            public StandardIDPool get(long partitionID) {
                return new StandardIDPool(idauth, partitionID, Integer.MAX_VALUE, 2000, 0.2);
            }
        }, 1000, 6, 100000);
    }

    @Test
    public void testStandardIDPool2() throws InterruptedException {
        final MockIDAuthority idauth = new MockIDAuthority(10000);
        idauth.setDelayAcquisition(2000);
        testIDPoolWith(new IDPoolFactory() {
            @Override
            public StandardIDPool get(long partitionID) {
                return new StandardIDPool(idauth, partitionID, Integer.MAX_VALUE, 4000, 0.1);
            }
        }, 2, 5, 10000);
    }

    @Test
    public void testStandardIDPool3() throws InterruptedException {
        final MockIDAuthority idauth = new MockIDAuthority(200);
        testIDPoolWith(new IDPoolFactory() {
            @Override
            public StandardIDPool get(long partitionID) {
                return new StandardIDPool(idauth, partitionID, Integer.MAX_VALUE, 2000, 0.2);
            }
        }, 10, 20, 100000);
    }

   @Test
    public void testStandardIDPoolConcurrencyOnSamePartition() throws InterruptedException {
        InMemoryKeyColumnValueStore store = new InMemoryKeyColumnValueStore("teststore");
        InMemoryStoreManager manager = new InMemoryStoreManager();
        testIDPoolWith((partitionID) -> {
            ConsistentKeyIDManager idauth;
            try{
             idauth = new ConsistentKeyIDManager(store,manager,new BaseConfiguration());
            }catch(StorageException se){
                throw new RuntimeException(se);
            }
            idauth.setIDBlockSizer((IDBlockSizer) (partition) ->{ return 10;});
            return new StandardIDPool(idauth, 0, Integer.MAX_VALUE, 2000, 0.2);
        }, 10, 100);
    }

    private void testIDPoolWith(IDPoolFactory poolFactory, final int numThreads, final int attemptsPerThread) throws InterruptedException {
        Thread[] threads = new Thread[numThreads];
        HashMap<Long, AtomicInteger> ids = new HashMap<Long, AtomicInteger>();
        AtomicInteger testFailed = new AtomicInteger();
        for(int i=0; i<numThreads; i++){
            threads[i] = new Thread((Runnable) () ->{
                Thread.yield();
                StandardIDPool idPool = poolFactory.get(0);
                for( int attempt=0; attempt<attemptsPerThread; attempt++){
                    long id = idPool.nextID();
                    //System.out.println("" + id + " " + attempt + " " + Thread.currentThread().getId() + " " + ids.get(id));
                    if(ids.get(id) != null)
                        testFailed.incrementAndGet();
                    assertTrue(ids.get(id) == null);
                    ids.put(id, new AtomicInteger());
                }
            });
            threads[i].start();
        }
        for(int i=0; i<numThreads; i++){
            threads[i].join();
        }
        assertFalse("Consistency Check failed " + testFailed.get() + " times",testFailed.get() > 0);
    }

    private void testIDPoolWith(IDPoolFactory poolFactory, final int numPartitions,
                                       final int numThreads, final int attemptsPerThread) throws InterruptedException {
        final Random random = new Random();
        final int[] partitions = new int[numPartitions];
        final IntSet[] ids = new IntSet[numPartitions];
        final StandardIDPool[] idPools = new StandardIDPool[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            partitions[i] = random.nextInt();
            ids[i] = new IntHashSet(attemptsPerThread * numThreads / numPartitions);
            idPools[i] = poolFactory.get(partitions[i]);
        }

        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int attempt = 0; attempt < attemptsPerThread; attempt++) {
                        int offset = random.nextInt(numPartitions);
                        int partition = partitions[offset];
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
            threads[i].run();
        }
        for (int i = 0; i < numThreads; i++) threads[i].join();
        for (int i = 0; i < idPools.length; i++) idPools[i].close();
        //Verify consecutive id assignment
        for (int i = 0; i < ids.length; i++) {
            IntSet set = ids[i];
            int max = 0;
            int[] all = set.getAll();
            for (int j=0;j<all.length;j++) if (all[j]>max) max=all[j];
            for (int j=1;j<=max;j++) assertTrue(set.contains(j));
        }
    }

    @Test
    public void testAllocationTimeout() {
        final MockIDAuthority idauth = new MockIDAuthority(10000);
        idauth.setDelayAcquisition(5000);
        StandardIDPool pool = new StandardIDPool(idauth, 1, Integer.MAX_VALUE, 4000, 0.1);
        try {
            pool.nextID();
            fail();
        } catch (TitanException e) {

        }

    }

    @Test
    public void testPoolExhaustion1() {
        MockIDAuthority idauth = new MockIDAuthority(200);
        int maxID = 10000;
        StandardIDPool pool = new StandardIDPool(idauth, 0, maxID, 2000, 0.2);
        for (int i = 1; i < maxID * 2; i++) {
            try {
                long id = pool.nextID();
                assertTrue(id <= maxID);
            } catch (IDPoolExhaustedException e) {
                assertEquals(maxID + 1, i);
                break;
            }
        }
    }

    @Test
    public void testPoolExhaustion2() {
        int maxID = 10000;
        MockIDAuthority idauth = new MockIDAuthority(200, maxID + 1);
        StandardIDPool pool = new StandardIDPool(idauth, 0, Integer.MAX_VALUE, 2000, 0.2);
        for (int i = 1; i < maxID * 2; i++) {
            try {
                long id = pool.nextID();
                assertTrue(id <= maxID);
            } catch (IDPoolExhaustedException e) {
                assertEquals(maxID + 1, i);
                break;
            }
        }
    }

    interface IDPoolFactory {
        public StandardIDPool get(long partitionID);
    }

}
