package com.thinkaurelius.titan.graphdb.idmanagement;

import com.thinkaurelius.titan.graphdb.database.idassigner.IDPoolExhaustedException;
import com.thinkaurelius.titan.graphdb.database.idassigner.StandardIDPool;
import com.thinkaurelius.titan.util.datastructures.IntHashSet;
import com.thinkaurelius.titan.util.datastructures.IntSet;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IDPoolTest {

    @Test
    public void testStandardIDPool() throws InterruptedException {
        final int numPartitions = 1000;
        final int numThreads = 6;
        final int attemptsPerThread = 100000;
        final Random random = new Random();

        MockIDAuthority idauth = new MockIDAuthority(200);
        final int[] partitions = new int[numPartitions];
        final IntSet[] ids = new IntSet[numPartitions];
        final StandardIDPool[] idPools = new StandardIDPool[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            partitions[i] = random.nextInt();
            ids[i] = new IntHashSet(attemptsPerThread * numThreads / numPartitions);
            idPools[i] = new StandardIDPool(idauth, partitions[i], Integer.MAX_VALUE);
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
    }

    @Test
    public void testPoolExhaustion1() {
        MockIDAuthority idauth = new MockIDAuthority(200);
        int maxID = 10000;
        StandardIDPool pool = new StandardIDPool(idauth, 0, maxID);
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
        StandardIDPool pool = new StandardIDPool(idauth, 0, Integer.MAX_VALUE);
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

}
