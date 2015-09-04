package com.thinkaurelius.titan.graphdb.idmanagement;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;

import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StandardStoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDPoolExhaustedException;
import com.thinkaurelius.titan.graphdb.database.idassigner.VertexIDAssigner;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static org.junit.Assert.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
    }

    private static TitanGraph getInMemoryGraph() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, InMemoryStoreManager.class.getCanonicalName());
        config.set(GraphDatabaseConfiguration.IDS_FLUSH, false);
        config.set(GraphDatabaseConfiguration.IDAUTHORITY_WAIT, Duration.ofMillis(1L));
        return TitanFactory.open(config);
    }

    @Test
    public void testIDAssignment() {
        LongSet vertexIds = new LongHashSet();
        LongSet relationIds = new LongHashSet();
        int totalRelations = 0;
        int totalVertices = 0;
        for (int trial = 0; trial < 10; trial++) {
            for (boolean flush : new boolean[]{true, false}) {
                TitanGraph graph = getInMemoryGraph();
                int numVertices = 1000;
                List<TitanVertex> vertices = new ArrayList<TitanVertex>(numVertices);
                List<InternalRelation> relations = new ArrayList<InternalRelation>();
                TitanVertex old = null;
                totalRelations+=2*numVertices;
                totalVertices+=numVertices;
                try {
                    for (int i = 0; i < numVertices; i++) {
                        TitanVertex next = graph.addVertex();
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
                    for (TitanVertex v : vertices) {
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


}
