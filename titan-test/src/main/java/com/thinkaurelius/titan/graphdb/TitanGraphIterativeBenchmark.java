package com.thinkaurelius.titan.graphdb;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StandardBaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.util.time.Timestamps;
import com.thinkaurelius.titan.graphdb.types.StandardEdgeLabelMaker;

import java.util.Random;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;

/**
 * A benchmark to test the performance of sequential row retrieval from the
 * underlying KeyColumnValueStore which is the basic operation underlying the
 * Fulgora OLAP component of Titan.
 * <p/>
 * Hence, this is effectively a benchmark for {@link com.thinkaurelius.titan.olap.OLAPTest}
 * or at least the primitive backend operations used therein.
 * <p/>
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanGraphIterativeBenchmark extends TitanGraphBaseTest {

    private static final Random random = new Random();


    public abstract KeyColumnValueStoreManager openStorageManager() throws StorageException;

    //@Test
    public void testDataSequential() throws Exception {
        loadData(200000,2);
        close();
        KeyColumnValueStoreManager manager = openStorageManager();
        KeyColumnValueStore store = manager.openDatabase(Backend.EDGESTORE_NAME);
        SliceQuery query = new SliceQuery(BufferUtil.zeroBuffer(8),BufferUtil.oneBuffer(8));
        query.setLimit(2);
        Stopwatch watch = new Stopwatch();
        watch.start();
        StoreTransaction txh = manager.beginTransaction(StandardBaseTransactionConfig.of(Timestamps.MILLI));
        KeyIterator iter = store.getKeys(query,txh);
        int numV = 0;
        while(iter.hasNext()) {
            StaticBuffer key = iter.next();
            RecordIterator<Entry> entries = iter.getEntries();
            assertEquals(2, Iterators.size(entries));
            numV++;
        }
        iter.close();
        txh.commit();
        System.out.println("Time taken: " + watch.elapsed(TimeUnit.MILLISECONDS));
        System.out.println("Num Vertices: " + numV);
        store.close();
        manager.close();

    }

    //@Test
    public void testLoadData() throws Exception {
        loadData(100000,2);
    }


    public void loadData(final int numVertices, final int numThreads) throws Exception {
        graph.makePropertyKey("w").dataType(Integer.class).make();
        PropertyKey time = graph.makePropertyKey("t").dataType(Long.class).make();
        ((StandardEdgeLabelMaker)graph.makeEdgeLabel("l")).sortKey(time).make();
        graph.commit();

        final int maxQueue = 1000;
        final int verticesPerTask = 1000;
        final int maxWeight = 10;
        final int maxTime = 10000;
        BlockingQueue<Runnable> tasks = new ArrayBlockingQueue<Runnable>(maxQueue);
        ExecutorService exe = Executors.newFixedThreadPool(numThreads);
        for (int i=0;i<numVertices/verticesPerTask;i++) {
            while (tasks.size()>=maxQueue) Thread.sleep(maxQueue);
            assert tasks.size()<maxQueue;
            exe.submit(new Runnable() {
                @Override
                public void run() {
                    TitanTransaction tx = graph.newTransaction();
                    TitanVertex[] vs = new TitanVertex[verticesPerTask];
                    for (int j=0;j<verticesPerTask;j++) {
                        vs[j]=tx.addVertex();
                        vs[j].setProperty("w", random.nextInt(maxWeight));
                    }
                    for (int j=0;j<verticesPerTask*10;j++) {
                        TitanEdge e = vs[random.nextInt(verticesPerTask)].addEdge("l",vs[random.nextInt(verticesPerTask)]);
                        e.setProperty("t",random.nextInt(maxTime));
                    }
                    System.out.print(".");
                    tx.commit();
                }
            });
        }
        exe.shutdown();
        exe.awaitTermination(numVertices/1000, TimeUnit.SECONDS);
        if (!exe.isTerminated()) System.err.println("Could not load data in time");
        System.out.println("Loaded "+numVertices+"vertices");
    }



}
