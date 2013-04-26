package com.thinkaurelius.titan.blueprints.persistit;

import com.thinkaurelius.titan.PersistitStorageSetup;
import com.thinkaurelius.titan.blueprints.TitanBlueprintsTest;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.persistit.PersistitKeyValueStore;
import com.thinkaurelius.titan.diskstorage.persistit.PersistitStoreManager;
import com.thinkaurelius.titan.diskstorage.persistit.PersistitTransaction;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.util.system.IOUtils;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

public class PersistitBlueprintsTest extends TitanBlueprintsTest {

    private static final String DEFAULT_DIR_NAME = "standard";
    private static Map<String, Graph> openGraphs = new HashMap<String, Graph>();

    @Override
    public Graph generateGraph() {
        return generateGraph(DEFAULT_DIR_NAME);
    }

    @Override
    public Graph generateGraph(final String subdir) {
        String dir = PersistitStorageSetup.getHomeDir() + "/" + subdir;
        Configuration config= new BaseConfiguration();
        config.subset(STORAGE_NAMESPACE).addProperty(STORAGE_DIRECTORY_KEY, dir);
        config.subset(STORAGE_NAMESPACE).addProperty(STORAGE_BACKEND_KEY, "persistit");
        Graph g = TitanFactory.open(config);
        synchronized (openGraphs) {
            openGraphs.put(dir, g);
        }
        return g;
    }

    @Override
    public boolean supportsMultipleGraphs() {
        return false;
    }

    @Override
    public void startUp() {
        //
    }

    @Override
    public void shutDown() {
        //
        synchronized (openGraphs) {
            for (String dir : openGraphs.keySet()) {
                File dirFile = new File(dir);
                Assert.assertFalse(dirFile.exists() && dirFile.listFiles().length > 0);
            }
        }
    }

    @Override
    public void cleanUp() throws StorageException {
        //
        synchronized (openGraphs) {
            for (Map.Entry<String, Graph> entry : openGraphs.entrySet()) {
                String dir = entry.getKey();
                Graph g = entry.getValue();
                g.shutdown();
                File dirFile = new File(dir);
                IOUtils.deleteDirectory(dirFile, true);
                Assert.assertFalse(dirFile.exists() && dirFile.listFiles().length > 0);
            }
        }
    }

    /**
     * Tests that transaction opened in one thread can be committed by another
     *
     * @throws Exception
     */
    public void testPersistitThreadedTx() throws Exception {
        String dir = PersistitStorageSetup.getHomeDir() + "/threadedtx";
        Configuration config = new BaseConfiguration();
        config.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, dir);
        final PersistitStoreManager mgr = new PersistitStoreManager(config);
        final PersistitKeyValueStore store = mgr.openDatabase("store");
        final Set<PersistitTransaction> txs = new HashSet<PersistitTransaction>();

        final CountDownLatch latchCommittedInOtherThread = new CountDownLatch(1);
        final CountDownLatch latchCommitInOtherThread = new CountDownLatch(1);

        // this thread starts a transaction then waits while the second thread tries to commit it.
        final Thread threadTxStarter = new Thread() {
            public void run() {
                ByteBuffer key = ByteBuffer.wrap(new byte[]{1});
                ByteBuffer val = ByteBuffer.wrap(new byte[]{2});

                try {
                    PersistitTransaction tx = mgr.beginTransaction(ConsistencyLevel.DEFAULT);
                    store.insert(key, val, tx);

                    latchCommitInOtherThread.countDown();

                    tx.abort();

                    try {
                        latchCommittedInOtherThread.await();
                    } catch (InterruptedException ie) {
                        throw new RuntimeException(ie);
                    }

                    PersistitTransaction tx2 = mgr.beginTransaction(ConsistencyLevel.DEFAULT);
                    txs.add(tx2);
                    Assert.assertFalse(store.containsKey(key, tx2));
                } catch (StorageException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        };

        threadTxStarter.start();

        // this thread tries to commit the transaction started in the first thread above.
        final Thread threadTryCommitTx = new Thread() {
            public void run() {
                try {
                    latchCommitInOtherThread.await();
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
                try {
                    PersistitTransaction tx = mgr.beginTransaction(ConsistencyLevel.DEFAULT);
                    tx.commit();
                } catch (StorageException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
                latchCommittedInOtherThread.countDown();
            }
        };

        threadTryCommitTx.start();

        threadTxStarter.join();
        threadTryCommitTx.join();

        for (PersistitTransaction tx : txs) {
//            store.containsKey(ByteBuffer.wrap(new byte[]{0}), tx);
//            store.insert(ByteBuffer.wrap(new byte[]{1}), ByteBuffer.wrap(new byte[]{2}), tx);
            tx.commit();
        }
    }

    @Test
    public void testTransactionIsolationCommitCheck() throws Exception {
        // the purpose of this test is to simulate rexster access to a graph instance, where one thread modifies
        // the graph and a separate thread cannot affect the transaction of the first
        final TransactionalGraph graph = (TransactionalGraph) generateGraph();

        if (!graph.getFeatures().isRDFModel) {
            final CountDownLatch latchCommittedInOtherThread = new CountDownLatch(1);
            final CountDownLatch latchCommitInOtherThread = new CountDownLatch(1);

            // this thread starts a transaction then waits while the second thread tries to commit it.
            final Thread threadTxStarter = new Thread() {
                public void run() {
                    final Vertex v = graph.addVertex(null);
                    v.setProperty("name", "stephen");

                    System.out.println("added vertex");

                    latchCommitInOtherThread.countDown();

                    try {
                        latchCommittedInOtherThread.await();
                    } catch (InterruptedException ie) {
                        throw new RuntimeException(ie);
                    }

                    graph.rollback();

                    // there should be no vertices here
                    System.out.println("reading vertex before tx");
                    Assert.assertFalse(graph.getVertices().iterator().hasNext());
                    System.out.println("read vertex before tx");
                }
            };

            threadTxStarter.start();

            // this thread tries to commit the transaction started in the first thread above.
            final Thread threadTryCommitTx = new Thread() {
                public void run() {
                    try {
                        latchCommitInOtherThread.await();
                    } catch (InterruptedException ie) {
                        throw new RuntimeException(ie);
                    }

                    // try to commit the other transaction
                    graph.commit();

                    latchCommittedInOtherThread.countDown();
                }
            };

            threadTryCommitTx.start();

            threadTxStarter.join();
            threadTryCommitTx.join();
        }

        graph.shutdown();

    }
}
