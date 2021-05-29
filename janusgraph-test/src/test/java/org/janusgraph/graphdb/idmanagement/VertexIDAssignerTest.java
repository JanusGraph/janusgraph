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
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.inmemory.InMemoryStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.idassigner.IDPoolExhaustedException;
import org.janusgraph.graphdb.database.idassigner.VertexIDAssigner;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexIDAssignerTest {

    public static Stream<Arguments> configs() {
        final List<Arguments> configurations = new ArrayList<>();

        for (int maxPerPartition : new int[]{Integer.MAX_VALUE, 100, 300}) {
            for (int numPartitions : new int[]{2, 4, 10}) {
                for (int[] local : new int[][]{null, {0,2, numPartitions}, {235,234,8}, {1,1,2}, {0,1<<(numPartitions-1),numPartitions}}) {
                    configurations.add(generateConfigurationArguments(numPartitions, maxPerPartition, local));
                }
            }
        }

        return configurations.stream();
    }

    /**
     *
     * @param numPartitionsBits The number of partitions bits to use. This means there are exactly (1<<numPartitionBits) partitions.
     * @param partitionMax The maximum number of ids that can be allocated per partition. This is artificially constrained by the MockIDAuthority
     * @param localPartitionDef This array contains three integers: 1+2) lower and upper bounds for the local partition range, and
     *                          3) the bit width of the local bounds. The bounds will be bit-shifted forward to consume the bit width
     */
    private static Arguments generateConfigurationArguments(int numPartitionsBits, int partitionMax, int[] localPartitionDef){
        MockIDAuthority idAuthority = new MockIDAuthority(11, partitionMax);

        StandardStoreFeatures.Builder fb = new StandardStoreFeatures.Builder();

        if (null != localPartitionDef) {
            fb.localKeyPartition(true);
            idAuthority.setLocalPartition(PartitionIDRangeTest.convert(localPartitionDef[0],localPartitionDef[1],localPartitionDef[2]));
        }
        StoreFeatures features = fb.build();

        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS,1<<numPartitionsBits);
        VertexIDAssigner idAssigner = new VertexIDAssigner(config, idAuthority, features);
        System.out.println(String.format("Configuration [%s|%s|%s]",numPartitionsBits,partitionMax,Arrays.toString(localPartitionDef)));

        long maxIDAssignments;
        if (localPartitionDef!=null && localPartitionDef[0]<localPartitionDef[1] && localPartitionDef[2]<=numPartitionsBits) {
            maxIDAssignments = ((localPartitionDef[1]-localPartitionDef[0])<<(numPartitionsBits-localPartitionDef[2]))*((long)partitionMax);
        } else {
            maxIDAssignments = (1<<numPartitionsBits)*((long)partitionMax);
        }

        return Arguments.arguments(idAssigner, maxIDAssignments, numPartitionsBits);
    }


    private enum CustomIdStrategy {
        LOW,
        HIGH
    }

    private JanusGraph getInMemoryGraph(boolean allowSettingVertexId, boolean idsFlush, int numPartitionsBits) {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, InMemoryStoreManager.class.getCanonicalName());
        config.set(GraphDatabaseConfiguration.IDS_FLUSH, idsFlush);
        config.set(GraphDatabaseConfiguration.IDAUTHORITY_WAIT, Duration.ofMillis(1L));
        config.set(GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS, 1<<numPartitionsBits);
        config.set(GraphDatabaseConfiguration.ALLOW_SETTING_VERTEX_ID, allowSettingVertexId);
        return JanusGraphFactory.open(config);
    }

    @Test
    public void testDisableIdsFlush() {
        final JanusGraph graph = getInMemoryGraph(false, false, 2);
        JanusGraphVertex v1 = graph.addVertex();
        JanusGraphVertex v2 = graph.addVertex();
        Edge e = v1.addEdge("knows", v2);
        e.property("prop", "old");
        graph.tx().commit();
        assertEquals("old", graph.traversal().E().next().property("prop").value());
        assertEquals(1, (long) graph.traversal().E().count().next());
        Object id = graph.traversal().E().next().id();

        // VertexIDAssigner shouldn't assign a new id if the edge is an existing one
        e = graph.traversal().E().next();
        e.property("prop", "new");
        graph.tx().commit();
        assertEquals("new", graph.traversal().E().next().property("prop").value());
        assertEquals(1, (long) graph.traversal().E().count().next());
        assertEquals(id, graph.traversal().E().next().id());
    }

    @ParameterizedTest
    @MethodSource("configs")
    public void testIDAssignment(VertexIDAssigner idAssigner, long maxIDAssignments, int numPartitionsBits) {
        LongSet vertexIds = new LongHashSet();
        LongSet relationIds = new LongHashSet();
        int totalRelations = 0;
        int totalVertices = 0;
        for (int trial = 0; trial < 10; trial++) {
            for (boolean flush : new boolean[]{true, false}) {
                final JanusGraph graph = getInMemoryGraph(false, false, numPartitionsBits);
                int numVertices = 1000;
                final List<JanusGraphVertex> vertices = new ArrayList<>(numVertices);
                final List<InternalRelation> relations = new ArrayList<>();
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
                    assertTrue(totalRelations>=maxIDAssignments/3*2 || totalVertices>=maxIDAssignments/3*2,
                        "Max Avail: " + maxIDAssignments + " vs. ["+totalVertices+","+totalRelations+"]");
                } finally {
                    graph.tx().rollback();
                    graph.close();
                }


            }
        }
    }

    @ParameterizedTest
    @MethodSource("configs")
    public void testCustomIdAssignment(VertexIDAssigner idAssigner, long maxIDAssignments, int numPartitionsBits) {
        testCustomIdAssignment(idAssigner, CustomIdStrategy.LOW, numPartitionsBits);
        testCustomIdAssignment(idAssigner, CustomIdStrategy.HIGH, numPartitionsBits);

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

    private void testCustomIdAssignment(VertexIDAssigner idAssigner, CustomIdStrategy idStrategy, int numPartitionsBits) {
        LongSet vertexIds = new LongHashSet();
        final long maxCount = idAssigner.getIDManager().getVertexCountBound();
        long count = 1;
        for (int trial = 0; trial < 10; trial++) {
            final JanusGraph graph = getInMemoryGraph(true, true, numPartitionsBits);
            int numVertices = 1000;
            final List<JanusGraphVertex> vertices = new ArrayList<>(numVertices);
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
