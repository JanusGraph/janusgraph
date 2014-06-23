package com.thinkaurelius.titan.graphdb;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;

import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.testcategory.PerformanceTests;
import com.thinkaurelius.titan.testutil.JUnitBenchmarkProvider;
import com.thinkaurelius.titan.testutil.RandomGenerator;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;
import com.tinkerpop.blueprints.util.wrappers.batch.VertexIDType;

@Category({ PerformanceTests.class })
public abstract class TitanWritePerformanceTest extends TitanGraphBaseTest {

    private BatchGraph<StandardTitanGraph> bgraph;

    private static final int BUFFER_SIZE = 1024;

    @Rule
    public TestRule benchmark = JUnitBenchmarkProvider.get();

    @Test
    public void testSimpleWriteWithoutBatch() {
        createSchema();
        new SimpleBGWrite().run();
    }

    @Test
    public void testSimpleWriteWithBatch() {
        createSchema();
        clopen(new TestConfigOption(GraphDatabaseConfiguration.STORAGE_BATCH), true);
        new SimpleBGWrite().run();
    }

    @Override
    public void open(WriteConfiguration config) {
        super.open(config);
        bgraph = new BatchGraph<StandardTitanGraph>(graph, VertexIDType.NUMBER, BUFFER_SIZE);
        bgraph.setVertexIdKey("uid");
    }

    @Override
    public void close() {
        if (mgmt!=null && mgmt.isOpen())
            mgmt.rollback();

        if (null != bgraph)
            bgraph.commit();

        //bgraph.shutdown();

        super.close();
    }

    private void createSchema() {
        PropertyKey weight = makeKey("weight", Double.class);
        PropertyKey uid    = makeVertexIndexedUniqueKey("uid", Integer.class);
        PropertyKey name   = makeVertexIndexedKey("name", String.class);
        EdgeLabel knows    = makeKeyedEdgeLabel("knows", uid, weight);
        finishSchema();
    }

    private class SimpleBGWrite implements Runnable {

        private final int noNodes;
        private final int noEdgesPerNode;

        private final String uidPrefix = "u";
        private final int uidLength = uidPrefix.length();

        private final StringBuilder sb = new StringBuilder(8);

        private SimpleBGWrite() {
            this(20000, 10);
        }

        private SimpleBGWrite(int noNodes, int noEdgesPerNode) {
            this.noNodes = noNodes;
            this.noEdgesPerNode = noEdgesPerNode;
            sb.append(uidPrefix);
        }

        public void run() {
            for (int i = 0; i < noNodes; i++) {
                Vertex v = bgraph.addVertex(i);
                v.setProperty("name", getName(i));
                v.setProperty("uid", i);
            }

            for (int i = 0; i < noNodes; i++) {
                // Out vertex
                Vertex n = bgraph.getVertex(i);

                // Iterate over in vertices
                for (int e = 0; e < noEdgesPerNode; e++) {
                    Vertex n2 = bgraph.getVertex(wrapAround(i + e, noNodes));
                    Edge edge = n.addEdge("knows", n2);
                    edge.setProperty("uid", RandomGenerator.randomInt(0, Integer.MAX_VALUE));
                    edge.setProperty("weight", Math.random());
                }
            }

            bgraph.commit();
        }

        private String getName(int index) {
            sb.setLength(uidLength);
            sb.append(index);
            return sb.toString();
        }
    }
//
//    private class SimpleWrite implements Runnable {
//
//        private final int noNodes;
//        private final int noEdgesPerNode;
//        private final int relationsPerTx;
//
//        private final String uidPrefix = "u";
//        private final int uidLength = uidPrefix.length();
//
//        private final StringBuilder sb = new StringBuilder(8);
//
//        private SimpleWrite() {
//            this(5000, 10, 5000);
//        }
//
//        private SimpleWrite(int noNodes, int noEdgesPerNode, int relationsPerTx) {
//            this.noNodes = noNodes;
//            this.noEdgesPerNode = noEdgesPerNode;
//            this.relationsPerTx = relationsPerTx;
//            sb.append(uidPrefix);
//        }
//
//        public void run() {
//            PropertyKey weight = makeKey("weight", Double.class);
//            PropertyKey uid    = makeVertexIndexedUniqueKey("uid", Integer.class);
//            PropertyKey name   = makeVertexIndexedKey("name", String.class);
//            EdgeLabel knows    = makeKeyedEdgeLabel("knows", uid, weight);
//            finishSchema();
//
//            int relationsInTx = 0;
//            for (int i = 0; i < noNodes; i++) {
//                TitanVertex v = tx.addVertex();
//                v.addProperty(name, sb.toString());
//                v.addProperty(uid, i);
//                relationsInTx += 2;
//                if (relationsInTx >= relationsPerTx) {
//                    newTx();
//                    relationsInTx = 0;
//                }
//            }
//
//            newTx();
//
//            relationsInTx = 0;
//
//            for (int i = 0; i < noNodes; i++) {
//                // Out vertex
//                TitanVertex n = tx.getVertices(uid, i).iterator().next();
//
//                // Iterate over in vertices
//                for (int e = 0; e < noEdgesPerNode; e++) {
//                    TitanVertex n2 = tx.getVertices(uid, wrapAround(i + e, noNodes)).iterator().next();
//                    TitanEdge r = n.addEdge(knows, n2);
//                    r.setProperty(uid, RandomGenerator.randomInt(0, Integer.MAX_VALUE));
//                    r.setProperty(weight, Math.random());
//                    if (relationsInTx >= relationsPerTx) {
//                        newTx();
//                        relationsInTx = 0;
//                    }
//                }
//            }
//
//            tx.commit();
//
//
////          TitanVertex v1 = (TitanVertex) Iterables.getOnlyElement(tx.getVertices("uid", 50));
////          TitanVertex v2 = (TitanVertex) Iterables.getOnlyElement(tx.getVertices("uid", 150));
////          assertTrue(v1.query().count() > 0);
////          assertEquals(v1.query().count(), v2.query().count());
//        }
//
//        private String getName(int index) {
//            sb.setLength(uidLength);
//            sb.append(index);
//            return sb.toString();
//        }
//    }
//
//
//
////    @Test
////    public void testInTxIndex() throws Exception {
////        makeVertexIndexedKey("uid",Long.class);
////        finishSchema();
////
////        int trials = 2;
////        int numV = 2000;
////        int offset = 10000;
////
////        long start = System.currentTimeMillis();
////        for (int t = 0; t < trials; t++) {
////            for (int i = offset; i < offset + numV; i++) {
////                if (Iterables.isEmpty(tx.getVertices("uid", Long.valueOf(i)))) {
////                    Vertex v = tx.addVertex();
////                    v.setProperty("uid", Long.valueOf(i));
////                }
////            }
////        }
////        assertEquals(numV, Iterables.size(tx.getVertices()));
////        System.out.println("Total time (ms): " + (System.currentTimeMillis() - start));
////    }


}
