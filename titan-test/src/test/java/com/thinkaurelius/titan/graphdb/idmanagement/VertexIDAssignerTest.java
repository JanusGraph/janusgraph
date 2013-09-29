package com.thinkaurelius.titan.graphdb.idmanagement;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.inmemory.InMemoryStorageAdapter;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.idassigner.VertexIDAssigner;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
        configurations.add(new Object[]{false, Integer.MAX_VALUE, null});

        for (int max : new int[]{Integer.MAX_VALUE, 100}) {
            for (int[] local : new int[][]{null, {0, 2000}, {-100000, -1}, {-10000, 10000}}) {
                configurations.add(new Object[]{true, max, local});
            }
        }

        return configurations;

    }

    public VertexIDAssignerTest(boolean partition, int partitionMax, int[] localPartition) {
        MockIDAuthority idAuthority = new MockIDAuthority(500, partitionMax);

        StoreFeatures features = new StoreFeatures();
        features.supportsUnorderedScan = false;
        features.supportsOrderedScan = false;
        features.supportsBatchMutation = false;
        features.supportsTransactions = false;
        features.supportsConsistentKeyOperations = false;
        features.supportsLocking = false;
        features.isKeyOrdered = false;
        features.isDistributed = false;
        features.hasLocalKeyPartition = false;
        if (localPartition != null) {
            features.hasLocalKeyPartition = true;
            idAuthority.setLocalPartition(localPartition);
        }
        Configuration config = new BaseConfiguration();
        config.setProperty(GraphDatabaseConfiguration.IDS_PARTITION_KEY, partition);
        idAssigner = new VertexIDAssigner(config, idAuthority, features);
        System.out.println("Partition: " + partition);
        System.out.println("partitionMax: " + partitionMax);
        System.out.println("localPartition: " + Arrays.toString(localPartition));
    }

    private static TitanGraph getInMemoryGraph() {
        BaseConfiguration config = new BaseConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, InMemoryStorageAdapter.class.getCanonicalName());
        config.subset(GraphDatabaseConfiguration.IDS_NAMESPACE).addProperty(GraphDatabaseConfiguration.IDS_FLUSH_KEY, false);
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
                        idAssigner.assignID((InternalVertex) next);
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
                        System.out.println(idAssigner.getIDManager().getPartitionID(v.getID()));
                    }
                    System.out.println("_____________________________________________");
                }
            }
        }
    }


}
