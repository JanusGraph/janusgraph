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

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphVertex;

import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.idassigner.IDPoolExhaustedException;
import org.janusgraph.graphdb.database.idassigner.VertexIDAssigner;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalVertex;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static org.junit.Assert.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

@RunWith(Parameterized.class)
public class VertexIDAssignerTest {

    final VertexIDAssigner idAssigner;

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        List<Object[]> configurations = new ArrayList<Object[]>();

        for (int maxPerPartition : new int[]{Integer.MAX_VALUE, 100, 300}) {
            for (int numPartitions : new int[]{2, 4, 10}) {
                for (int[] local : new int[][]{null, {0,2, numPartitions}, {235,234,8}, {1,1,2}, {0,1<<(numPartitions-1),numPartitions}}) {
                    configurations.add(new Object[]{numPartitions, maxPerPartition, local});
                }
            }
        }

        return configurations;
    }

    private final long maxIDAssignments;

    private final long numPartitionsBits;

    private enum CustomIdStrategy {
        LOW,
        HIGH
    }

    /**
     *
     * @param numPartitionsBits The number of partitions bits to use. This means there are exactly (1<<numPartitionBits) partitions.
     * @param partitionMax The maxium number of ids that can be allocated per partition. This is artifically constraint by the MockIDAuthority
     * @param localPartitionDef This array contains three integers: 1+2) lower and upper bounds for the local partition range, and
     *                          3) the bit width of the local bounds. The bounds will be bitshifted forward to consume the bit width
     */
    public VertexIDAssignerTest(int numPartitionsBits, int partitionMax, int[] localPartitionDef) {
        MockIDAuthority idAuthority = new MockIDAuthority(11, partitionMax);

        StandardStoreFeatures.Builder fb = new StandardStoreFeatures.Builder();

        if (null != localPartitionDef) {
            fb.localKeyPartition(true);
            idAuthority.setLocalPartition(PartitionIDRangeTest.convert(localPartitionDef[0],localPartitionDef[1],localPartitionDef[2]));
        }
        StoreFeatures features = fb.build();

        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS,1<<numPartitionsBits);
        idAssigner = new VertexIDAssigner(config, idAuthority, features);
        System.out.println(String.format("Configuration [%s|%s|%s]",numPartitionsBits,partitionMax,Arrays.toString(localPartitionDef)));

        if (localPartitionDef!=null && localPartitionDef[0]<localPartitionDef[1] && localPartitionDef[2]<=numPartitionsBits) {
            this.maxIDAssignments = ((localPartitionDef[1]-localPartitionDef[0])<<(numPartitionsBits-localPartitionDef[2]))*((long)partitionMax);
        } else {
            this.maxIDAssignments = (1<<numPartitionsBits)*((long)partitionMax);
        }

        this.numPartitionsBits = numPartitionsBits;
    }

    private JanusGraph getInMemoryGraph(boolean allowSettingVertexId, boolean idsFlush) {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, InMemoryStoreManager.class.getCanonicalName());
        config.set(GraphDatabaseConfiguration.IDS_FLUSH, idsFlush);
        config.set(GraphDatabaseConfiguration.IDAUTHORITY_WAIT, Duration.ofMillis(1L));
        config.set(GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS, 1<<numPartitionsBits);
        config.set(GraphDatabaseConfiguration.ALLOW_SETTING_VERTEX_ID, allowSettingVertexId);
        return JanusGraphFactory.open(config);
    }

    @Test
    public void testIDAssignment() {
        LongSet vertexIds = new LongHashSet();
        LongSet relationIds = new LongHashSet();
        int totalRelations = 0;
        int totalVertices = 0;
        for (int trial = 0; trial < 10; trial++) {
            for (boolean flush : new boolean[]{true, false}) {
                JanusGraph graph = getInMemoryGraph(false, false);
                int numVertices = 1000;
                List<JanusGraphVertex> vertices = new ArrayList<JanusGraphVertex>(numVertices);
                List<InternalRelation> relations = new ArrayList<InternalRelation>();
                JanusGraphVertex old = null;
                totalRelations+=2*numVertices;
                totalVertices+=numVertices;
                try {
                    for (int i = 0; i < numVertices; i++) {
                        JanusGraphVertex next = graph.addVertex();
                        InternalRelation edge = null;
                        if (old != null) {
                            edge = (InternalRelation) old.addEdge("knows", next);
                        }
                        InternalRelation property = (InternalRelation) next.property("age", 25);
                        if (flush) {
                            idAssigner.assignID((InternalVertex) next, next.vertexLabel());
                            idAssigner.assignID(property);
                            if (edge != null) idAssigner.assignID(edge);
                        }
                        relations.add(property);
                        if (edge != null) relations.add(edge);
                        vertices.add(next);
                        old = next;
                    }
                    if (!flush) idAssigner.assignIDs(relations);
                    //Check if we should have exhausted the id pools
                    if (totalRelations>maxIDAssignments || totalVertices>maxIDAssignments) fail();

                    //Verify that ids are set and unique
                    for (JanusGraphVertex v : vertices) {
                        assertTrue(v.hasId());
                        long id = v.longId();
                        assertTrue(id>0 && id<Long.MAX_VALUE);
                        assertTrue(vertexIds.add(id));
                    }
                    for (InternalRelation r : relations) {
                        assertTrue(r.hasId());
                        long id = r.longId();
                        assertTrue(id>0 && id<Long.MAX_VALUE);
                        assertTrue(relationIds.add(id));
                    }
                } catch (IDPoolExhaustedException e) {
                    //Since the id assignment process is randomized, we divide by 3/2 to account for minor variations
                    assertTrue("Max Avail: " + maxIDAssignments + " vs. ["+totalVertices+","+totalRelations+"]",
                            totalRelations>=maxIDAssignments/3*2 || totalVertices>=maxIDAssignments/3*2);
                } finally {
                    graph.tx().rollback();
                    graph.close();
                }


            }
        }
    }

    @Test
    public void testCustomIdAssignment() {
        testCustomIdAssignment(CustomIdStrategy.LOW);
        testCustomIdAssignment(CustomIdStrategy.HIGH);

        final IDManager idManager = idAssigner.getIDManager();
        for (final long id : new long[] {0, idManager.getVertexCountBound()}) {
            try {
                idManager.toVertexId(id);
                fail("Should fail to convert out of range user id to graph vertex id");
            } catch (IllegalArgumentException e) {
                // should throw this exception
            }
        }

        for (final long vertexId : new long[] {idManager.toVertexId(1)-1, idManager.toVertexId(idManager.getVertexCountBound()-1)+1}) {
            try {
                idManager.fromVertexId(vertexId);
                fail("Should fail to convert out of range vertex id to user id");
            } catch (IllegalArgumentException e) {
                // should throw this exception
            }
        }
    }

    private void testCustomIdAssignment(CustomIdStrategy idStrategy) {
        LongSet vertexIds = new LongHashSet();
        final long maxCount = idAssigner.getIDManager().getVertexCountBound();
        long count = 1;
        for (int trial = 0; trial < 10; trial++) {
            JanusGraph graph = getInMemoryGraph(true, true);
            int numVertices = 1000;
            List<JanusGraphVertex> vertices = new ArrayList<JanusGraphVertex>(numVertices);
            try {
                for (int i = 0; i < numVertices; i++, count++) {
                    final long userVertexId;
                    switch (idStrategy) {
                        case LOW:
                            userVertexId = count;
                            break;
                        case HIGH:
                            userVertexId = maxCount-count;
                            break;
                        default:
                            throw new RuntimeException("Unsupported custom id strategy: " + idStrategy);
                    }
                    final long id = idAssigner.getIDManager().toVertexId(userVertexId);
                    JanusGraphVertex next = graph.addVertex(T.id, id, "user_id", userVertexId);
                    vertices.add(next);
                }

                //Verify that ids are set, unique and consistent with user id basis
                for (JanusGraphVertex v : vertices) {
                    assertTrue(v.hasId());
                    long id = v.longId();
                    assertTrue(id>0 && id<Long.MAX_VALUE);
                    assertTrue(vertexIds.add(id));
                    assertEquals((long) v.value("user_id"), idAssigner.getIDManager().fromVertexId(id));
                }
            } finally {
                graph.tx().rollback();
                graph.close();
            }
        }
    }

}
