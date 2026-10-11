// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.diskstorage.berkeleyje;

import com.sleepycat.je.StatsConfig;
import org.janusgraph.BerkeleyStorageSetup;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.IDAuthority;
import org.janusgraph.diskstorage.IDBlock;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The ID authority of a graph on BerkeleyJE claims its ID blocks one at a time, whatever their partitions and id
 * namespaces. A BerkeleyJE transaction keeps the records it reads locked until it ends, and the rows of the claims of
 * different partitions and namespaces are neighbours in the id store, so claims in parallel wait for each other's
 * commits, and fail where a commit takes longer than the lock timeout. The environment counts every wait for a lock,
 * which claims one at a time never make, and a lock timeout of a millisecond fails a claim which waits longer.
 */
public class BerkeleyIDBlockClaimsTest {

    private static final int CLAIMANTS = 8;
    private static final int PARTITIONS = 8;
    //The id namespaces of the graph's pools: normal, unmodifiable and partitioned vertices, relations and schema
    private static final int NAMESPACES = 5;
    private static final int ROUNDS = 3;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @TempDir
    Path directory;

    private JanusGraph graph;
    private ExecutorService claimants;

    @AfterEach
    public void tearDown() throws InterruptedException {
        try {
            if (claimants != null) {
                //Without interrupting a claim: an interruption during its I/O would invalidate the environment
                claimants.shutdown();
                assertTrue(claimants.awaitTermination(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "a claim didn't end");
            }
        } finally {
            if (graph != null) {
                graph.close();
            }
        }
    }

    @Test
    public void claimsOfDifferentPartitionsAndNamespacesDoNotWaitForEachOthersLocks() throws Exception {
        graph = JanusGraphFactory.open(BerkeleyStorageSetup.getBerkeleyJEConfiguration(directory.toString())
            .set(BerkeleyJEStoreManager.EXT_LOCK_TIMEOUT, "1 ms").getConfiguration());
        final IDAuthority authority = ((StandardJanusGraph) graph).getBackend().getIDAuthority();
        claimants = Executors.newFixedThreadPool(CLAIMANTS);
        final long lockWaitsBefore = lockWaits();

        final List<Future<IDBlock>> claims = new ArrayList<>(ROUNDS * PARTITIONS * NAMESPACES);
        for (int round = 0; round < ROUNDS; round++) {
            for (int partition = 0; partition < PARTITIONS; partition++) {
                for (int namespace = 0; namespace < NAMESPACES; namespace++) {
                    final int claimPartition = partition;
                    final int claimNamespace = namespace;
                    claims.add(claimants.submit(() -> authority.getIDBlock(claimPartition, claimNamespace, TIMEOUT)));
                }
            }
        }
        for (Future<IDBlock> claim : claims) {
            assertTrue(claim.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).numIds() > 0);
        }
        assertEquals(lockWaitsBefore, lockWaits(), "a claim waited for the lock of another");
    }

    //The waits for a lock in the graph's BerkeleyJE environment so far
    private long lockWaits() {
        final OrderedKeyValueStoreManagerAdapter storeManager =
            (OrderedKeyValueStoreManagerAdapter) ((StandardJanusGraph) graph).getBackend().getStoreManager();
        return ((BerkeleyJEStoreManager) storeManager.getManager()).getEnvironment().getStats(new StatsConfig())
            .getNWaits();
    }
}
