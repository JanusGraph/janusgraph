// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.idmanagement;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.IDAuthority;
import org.janusgraph.diskstorage.IDBlock;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.graphdb.database.idassigner.IDBlockSizer;
import org.janusgraph.graphdb.database.idassigner.IDPoolExhaustedException;
import org.janusgraph.graphdb.database.idassigner.StandardIDPool;
import org.janusgraph.graphdb.util.IntHashSet;
import org.janusgraph.graphdb.util.IntSet;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Random;

import static org.easymock.EasyMock.expect;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IDPoolTest {



    @Test
    public void testStandardIDPool1() throws InterruptedException {

        final MockIDAuthority idAuthority = new MockIDAuthority(200);
        testIDPoolWith(partitionID -> new StandardIDPool(idAuthority, partitionID, partitionID, Integer.MAX_VALUE, Duration.ofMillis(2000L), 0.2), 1000, 6, 100000);
    }

    @Test
    public void testStandardIDPool2() throws InterruptedException {
        final MockIDAuthority idAuthority = new MockIDAuthority(10000, Integer.MAX_VALUE, 2000);
        testIDPoolWith(partitionID -> new StandardIDPool(idAuthority, partitionID, partitionID, Integer.MAX_VALUE, Duration.ofMillis(4000), 0.1), 2, 5, 10000);
    }

    @Test
    public void testStandardIDPool3() throws InterruptedException {
        final MockIDAuthority idAuthority = new MockIDAuthority(200);
        testIDPoolWith(partitionID -> new StandardIDPool(idAuthority, partitionID, partitionID, Integer.MAX_VALUE, Duration.ofMillis(2000), 0.2), 10, 20, 100000);
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

            threads[i] = new Thread(() -> {
                for (int attempt = 0; attempt < attemptsPerThread; attempt++) {
                    int offset = random.nextInt(numPartitions);
                    long id = idPools[offset].nextID();
                    assertTrue(id < Integer.MAX_VALUE);
                    IntSet idSet = ids[offset];
                    synchronized (idSet) {
                        assertFalse(idSet.contains((int) id));
                        idSet.add((int) id);
                    }
                }
            });
            threads[i].start();
        }
        for (int i = 0; i < numThreads; i++) threads[i].join();
        for (final StandardIDPool idPool : idPools) idPool.close();
        //Verify consecutive id assignment
        for (int i = 0; i < ids.length; i++) {
            IntSet set = ids[i];
            int max = 0;
            int[] all = set.getAll();
            for (int id : all) if (id > max) max = id;
            for (int j=1;j<=max;j++) assertTrue(set.contains(j), i+ " contains: " + j);
        }
    }

    @Test
    public void testAllocationTimeout() {
        final MockIDAuthority idAuthority = new MockIDAuthority(10000, Integer.MAX_VALUE, 5000);
        StandardIDPool pool = new StandardIDPool(idAuthority, 1, 1, Integer.MAX_VALUE, Duration.ofMillis(4000), 0.1);

        assertThrows(JanusGraphException.class, pool::nextID);
    }

    @Test
    public void testAllocationTimeoutAndRecovery() throws BackendException {
        IMocksControl ctrl = EasyMock.createStrictControl();

        final int partition = 42;
        final int idNamespace = 777;
        final Duration timeout = Duration.ofSeconds(1L);

        final IDAuthority mockAuthority = ctrl.createMock(IDAuthority.class);

        // Sleep for two seconds, then throw a BackendException
        // this whole delegate could be deleted if we abstracted StandardIDPool's internal executor and stopwatches
        expect(mockAuthority.getIDBlock(partition, idNamespace, timeout)).andDelegateTo(new IDAuthority() {
            @Override
            public IDBlock getIDBlock(int partition, int idNamespace, Duration timeout) throws BackendException {
                assertThrows(InterruptedException.class, () -> Thread.sleep(2000L));
                throw new TemporaryBackendException("slow backend");
            }

            @Override
            public List<KeyRange> getLocalIDPartition() {
                throw new IllegalArgumentException();
            }

            @Override
            public void setIDBlockSizer(IDBlockSizer sizer) {
                throw new IllegalArgumentException();
            }

            @Override
            public void close() {
                throw new IllegalArgumentException();
            }

            @Override
            public String getUniqueID() {
                throw new IllegalArgumentException();
            }

            @Override
            public boolean supportsInterruption()
            {
                return true;
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
        expect(mockAuthority.supportsInterruption()).andStubReturn(true);

        ctrl.replay();
        StandardIDPool pool = new StandardIDPool(mockAuthority, partition, idNamespace, Integer.MAX_VALUE, timeout, 0.1);

        assertThrows(JanusGraphException.class, pool::nextID);

        long nextID = pool.nextID();
        assertEquals(200, nextID);

        ctrl.verify();
    }

    @Test
    public void testPoolExhaustion1() {
        MockIDAuthority idAuthority = new MockIDAuthority(200);
        int idUpper = 10000;
        StandardIDPool pool = new StandardIDPool(idAuthority, 0, 1, idUpper, Duration.ofMillis(2000), 0.2);
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
        MockIDAuthority idAuthority = new MockIDAuthority(200, idUpper);
        StandardIDPool pool = new StandardIDPool(idAuthority, 0, 1, Integer.MAX_VALUE, Duration.ofMillis(2000), 0.2);
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
        StandardIDPool get(int partitionID);
    }

}
