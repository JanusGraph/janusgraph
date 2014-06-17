package com.thinkaurelius.titan.graphdb.idmanagement;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StandardStoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.idassigner.VertexIDAssigner;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
        configurations.add(new Object[]{false, Integer.MAX_VALUE, null});

        for (int max : new int[]{Integer.MAX_VALUE, 100}) {
            for (int[] local : new int[][]{null, {0, 2000}, {-100000, -1}, {10000, -10000}}) {
                configurations.add(new Object[]{true, max, local});
            }
        }

        return configurations;
    }

    public VertexIDAssignerTest(Boolean partition, int partitionMax, int[] localPartition) {
        MockIDAuthority idAuthority = new MockIDAuthority(11, partitionMax);

        StandardStoreFeatures.Builder fb = new StandardStoreFeatures.Builder();

        if (null != localPartition) {
            fb.localKeyPartition(true);
            idAuthority.setLocalPartition(localPartition);
        }
        StoreFeatures features = fb.build();

        ModifiableConfiguration config = GraphDatabaseConfiguration.buildConfiguration();
        config.set(GraphDatabaseConfiguration.CLUSTER_PARTITION,partition);
        config.set(GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS,1024);
        idAssigner = new VertexIDAssigner(config, idAuthority, features);
        System.out.println("Partition: " + partition);
        System.out.println("partitionMax: " + partitionMax);
        System.out.println("localPartition: " + Arrays.toString(localPartition));
    }

    private static TitanGraph getInMemoryGraph() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, InMemoryStoreManager.class.getCanonicalName());
        config.set(GraphDatabaseConfiguration.IDS_FLUSH, false);
        config.set(GraphDatabaseConfiguration.IDAUTHORITY_WAIT, new StandardDuration(1L, TimeUnit.MILLISECONDS));
        return TitanFactory.open(config);
    }

    @Test
    public void testIDAssignment() {
        for (int trial = 0; trial < 100; trial++) {
            for (boolean flush : new boolean[]{true, false}) {
                TitanGraph graph = getInMemoryGraph();
                int numVertices = 100;
                List<TitanVertex> vertices = new ArrayList<TitanVertex>(numVertices);
                List<InternalRelation> relations = new ArrayList<InternalRelation>();
                TitanVertex old = null;
                for (int i = 0; i < numVertices; i++) {
                    TitanVertex next = (TitanVertex) graph.addVertex(null);
                    InternalRelation edge = null;
                    if (old != null) {
                        edge = (InternalRelation) graph.addEdge(null, old, next, "knows");
                    }
                    InternalRelation property = (InternalRelation) next.addProperty("age", 25);
                    if (flush) {
                        idAssigner.assignID((InternalVertex) next,next.getVertexLabel());
                        idAssigner.assignID(property);
                        if (edge != null) idAssigner.assignID(edge);
                    } else {
                        relations.add(property);
                        if (edge != null) relations.add(edge);
                    }
                    vertices.add(next);
                    old = next;
                }
                if (!flush) idAssigner.assignIDs(relations);
                if (trial == -1) {
                    for (TitanVertex v : vertices) {
                        System.out.println(idAssigner.getIDManager().getPartitionId(v.getID()));
                    }
                    System.out.println("_____________________________________________");
                }
                graph.rollback();
                graph.shutdown();
            }
        }
    }


}
