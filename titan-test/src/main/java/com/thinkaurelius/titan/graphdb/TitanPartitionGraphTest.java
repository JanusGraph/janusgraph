package com.thinkaurelius.titan.graphdb;


import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.idassigner.placement.SimpleBulkPlacementStrategy;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.testcategory.OrderedKeyStoreTests;
import com.tinkerpop.blueprints.Direction;
import org.junit.Test;
import org.junit.experimental.categories.Category;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Tests graph and vertex partitioning
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@Category({ OrderedKeyStoreTests.class })
public abstract class TitanPartitionGraphTest extends TitanGraphBaseTest {


    public abstract WriteConfiguration getBaseConfiguration();

    @Override
    public WriteConfiguration getConfiguration() {
        WriteConfiguration config = getBaseConfiguration();
        ModifiableConfiguration mconf = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,config, BasicConfiguration.Restriction.NONE);
        mconf.set(GraphDatabaseConfiguration.CLUSTER_PARTITION,true);
        mconf.set(GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS,8);
        //uses SimpleBulkPlacementStrategy by default
        mconf.set(SimpleBulkPlacementStrategy.CONCURRENT_PARTITIONS,16);
        return config;
    }

    @Test
    public void testSetup() {
        final IDManager idManager = graph.getIDManager();
        assertEquals(8, idManager.getPartitionBound());
    }

    @Test
    public void testVertexPartitioning() {
        PropertyKey gid = makeVertexIndexedUniqueKey("gid",Integer.class);
        PropertyKey sig = makeKey("sig",Integer.class);
        PropertyKey name = mgmt.makePropertyKey("name").cardinality(Cardinality.LIST).dataType(String.class).make();
        EdgeLabel knows = makeLabel("knows");

        VertexLabel person = mgmt.makeVertexLabel("person").make();
        VertexLabel group = mgmt.makeVertexLabel("group").partition().make();

        finishSchema();
        final IDManager idManager = graph.getIDManager();
        final Set<String> names = ImmutableSet.of("Marko","Dan","Stephen","Daniel","Josh","Thad","Pavel","Matthias");

        TitanVertex g = tx.addVertex("group");
        g.setProperty("gid", 1);
        for (String n : names) g.addProperty("name",n);
        newTx();
        assertTrue(g.hasId());
        long gId = g.getID();
        assertTrue(idManager.isPartitionedVertex(gId));
        assertEquals(idManager.getCanonicalVertexId(gId),gId);
        final int canonicalPartition = getPartitionID(g,idManager);
        g = tx.getVertex(gId);
        assertEquals(g,Iterables.getOnlyElement(tx.query().has("gid",1).vertices()));
        //Verify that properties are distributed correctly
        TitanProperty p = Iterables.getOnlyElement(g.getProperties("gid"));
        assertEquals(canonicalPartition,getPartitionID(p,idManager));
        Set<Integer> propPartitions = Sets.newHashSet();
        for (TitanProperty n : g.getProperties("name")) {
            propPartitions.add(getPartitionID(n,idManager));
        }
        //Verify spread across partitions; this number is a pessimistic lower bound but might fail since it is probabilistic
        assertTrue(propPartitions.size()>=3);

        newTx();
        final int numTx = 100;
        final int vPerTx = 10;
        List<Integer> partitions = new ArrayList<Integer>(numTx);

        for (int t=0;t<numTx;t++) {
            g = tx.getVertex(gId);
            assertNotNull(g);
            TitanVertex[] vs = new TitanVertex[vPerTx];
            for (int vi=0;vi<vPerTx;vi++) {
                vs[vi] = tx.addVertex("person");
                vs[vi].setProperty("sig", t);
                TitanEdge e = vs[vi].addEdge("knows",g);
                e.setProperty("sig",t);
                e = g.addEdge("knows",vs[vi]);
                e.setProperty("sig",t);
            }
            newTx();
            //Verify that all elements are in the same partition
            TitanTransaction txx = graph.buildTransaction().readOnly().start();
            g = tx.getVertex(gId);
            int partition = -1;
            for (int vi=0;vi<vPerTx;vi++) {
                assertTrue(vs[vi].hasId());
                int pid = getPartitionID(vs[vi],idManager);
                if (partition<0) partition=pid;
                else assertEquals(partition,pid);
                int numRels = 0;
                TitanVertex v = txx.getVertex(vs[vi].getID());
                for (TitanRelation r : v.query().relations()) {
                    numRels++;
                    assertEquals(partition,getPartitionID(r,idManager));
                    if (r instanceof TitanEdge) {
                        assertEquals(g,((TitanEdge)r).getOtherVertex(v));
                    }
                }
                assertEquals(3,numRels);
            }
            txx.commit();
        }
        //Verify spread across partitions; this number is a pessimistic lower bound but might fail since it is probabilistic
        assertTrue(Sets.newHashSet(partitions).size()>=3); //

        newTx();
        //Verify edge querying across partitions
        g = tx.getVertex(gId);
        assertEquals(1,g.getProperty("gid"));
        assertEquals(names.size(),Iterables.size(g.getProperties("name")));
        assertEquals(numTx*vPerTx,Iterables.size(g.getEdges(Direction.OUT, "knows")));
        assertEquals(numTx*vPerTx,Iterables.size(g.getEdges(Direction.IN, "knows")));
        assertEquals(numTx*vPerTx*2,Iterables.size(g.getEdges(Direction.BOTH,"knows")));
    }


    public static int getPartitionID(TitanVertex vertex, IDManager idManager) {
        long p = idManager.getPartitionId(vertex.getID());
        assertTrue(p>=0 && p<idManager.getPartitionBound() && p<Integer.MAX_VALUE);
        return (int)p;
    }

    public static int getPartitionID(TitanRelation relation, IDManager idManager) {
        long p = relation.getID() & (idManager.getPartitionBound()-1);
        assertTrue(p>=0 && p<idManager.getPartitionBound() && p<Integer.MAX_VALUE);
        return (int)p;
    }

}
