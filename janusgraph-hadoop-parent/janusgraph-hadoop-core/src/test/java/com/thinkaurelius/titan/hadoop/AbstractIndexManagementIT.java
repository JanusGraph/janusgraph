package com.thinkaurelius.titan.hadoop;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.RelationTypeIndex;
import com.thinkaurelius.titan.core.schema.SchemaAction;
import com.thinkaurelius.titan.core.schema.SchemaStatus;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.example.GraphOfTheGodsFactory;
import com.thinkaurelius.titan.graphdb.TitanGraphBaseTest;
import com.thinkaurelius.titan.graphdb.database.management.ManagementSystem;
import com.thinkaurelius.titan.graphdb.olap.job.IndexRemoveJob;
import com.thinkaurelius.titan.graphdb.olap.job.IndexRepairJob;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractIndexManagementIT extends TitanGraphBaseTest {

    @Test
    public void testRemoveGraphIndex() throws InterruptedException, BackendException, ExecutionException {
        tx.commit();
        mgmt.commit();

        // Load the "Graph of the Gods" sample data
        GraphOfTheGodsFactory.loadWithoutMixedIndex(graph, true);

        // Disable the "name" composite index
        TitanManagement m = graph.openManagement();
        TitanGraphIndex nameIndex = m.getGraphIndex("name");
        m.updateIndex(nameIndex, SchemaAction.DISABLE_INDEX);
        m.commit();
        graph.tx().commit();

        // Block until the SchemaStatus transitions to DISABLED
        assertTrue(ManagementSystem.awaitGraphIndexStatus(graph, "name")
                .status(SchemaStatus.DISABLED).call().getSucceeded());

        // Remove index
        MapReduceIndexManagement mri = new MapReduceIndexManagement(graph);
        m = graph.openManagement();
        TitanGraphIndex index = m.getGraphIndex("name");
        ScanMetrics metrics = mri.updateIndex(index, SchemaAction.REMOVE_INDEX).get();

        assertEquals(12, metrics.getCustom(IndexRemoveJob.DELETED_RECORDS_COUNT));
    }

    @Test
    public void testRemoveRelationIndex() throws InterruptedException, BackendException, ExecutionException {
        tx.commit();
        mgmt.commit();

        // Load the "Graph of the Gods" sample data
        GraphOfTheGodsFactory.loadWithoutMixedIndex(graph, true);

        // Disable the "battlesByTime" index
        TitanManagement m = graph.openManagement();
        RelationType battled = m.getRelationType("battled");
        RelationTypeIndex battlesByTime = m.getRelationIndex(battled, "battlesByTime");
        m.updateIndex(battlesByTime, SchemaAction.DISABLE_INDEX);
        m.commit();
        graph.tx().commit();

        // Block until the SchemaStatus transitions to DISABLED
        assertTrue(ManagementSystem.awaitRelationIndexStatus(graph, "battlesByTime", "battled")
                .status(SchemaStatus.DISABLED).call().getSucceeded());

        // Remove index
        MapReduceIndexManagement mri = new MapReduceIndexManagement(graph);
        m = graph.openManagement();
        battled = m.getRelationType("battled");
        battlesByTime = m.getRelationIndex(battled, "battlesByTime");
        ScanMetrics metrics = mri.updateIndex(battlesByTime, SchemaAction.REMOVE_INDEX).get();

        assertEquals(6, metrics.getCustom(IndexRemoveJob.DELETED_RECORDS_COUNT));
    }

    @Test
    public void testRepairGraphIndex() throws InterruptedException, BackendException, ExecutionException {
        tx.commit();
        mgmt.commit();

        // Load the "Graph of the Gods" sample data (WITHOUT mixed index coverage)
        GraphOfTheGodsFactory.loadWithoutMixedIndex(graph, true);

        // Create and enable a graph index on age
        TitanManagement m = graph.openManagement();
        PropertyKey age = m.getPropertyKey("age");
        m.buildIndex("verticesByAge", Vertex.class).addKey(age).buildCompositeIndex();
        m.commit();
        graph.tx().commit();

        // Block until the SchemaStatus transitions to REGISTERED
        assertTrue(ManagementSystem.awaitGraphIndexStatus(graph, "verticesByAge")
                .status(SchemaStatus.REGISTERED).call().getSucceeded());

        m = graph.openManagement();
        TitanGraphIndex index = m.getGraphIndex("verticesByAge");
        m.updateIndex(index, SchemaAction.ENABLE_INDEX);
        m.commit();
        graph.tx().commit();

        // Block until the SchemaStatus transitions to ENABLED
        assertTrue(ManagementSystem.awaitGraphIndexStatus(graph, "verticesByAge")
                .status(SchemaStatus.ENABLED).call().getSucceeded());

        // Run a query that hits the index but erroneously returns nothing because we haven't repaired yet
        assertFalse(graph.query().has("age", 10000).vertices().iterator().hasNext());

        // Repair
        MapReduceIndexManagement mri = new MapReduceIndexManagement(graph);
        m = graph.openManagement();
        index = m.getGraphIndex("verticesByAge");
        ScanMetrics metrics = mri.updateIndex(index, SchemaAction.REINDEX).get();
        assertEquals(6, metrics.getCustom(IndexRepairJob.ADDED_RECORDS_COUNT));

        // Test the index
        Iterable<TitanVertex> hits = graph.query().has("age", 4500).vertices();
        assertNotNull(hits);
        assertEquals(1, Iterables.size(hits));
        TitanVertex v = Iterables.getOnlyElement(hits);
        assertNotNull(v);

        assertEquals("neptune", v.value("name"));
    }

    @Test
    public void testRepairRelationIndex() throws InterruptedException, BackendException, ExecutionException {
        tx.commit();
        mgmt.commit();

        // Load the "Graph of the Gods" sample data (WITHOUT mixed index coverage)
        GraphOfTheGodsFactory.loadWithoutMixedIndex(graph, true);

        // Create and enable a relation index on lives edges by reason
        TitanManagement m = graph.openManagement();
        PropertyKey reason = m.getPropertyKey("reason");
        EdgeLabel lives = m.getEdgeLabel("lives");
        m.buildEdgeIndex(lives, "livesByReason", Direction.BOTH, Order.decr, reason);
        m.commit();
        graph.tx().commit();

        // Block until the SchemaStatus transitions to REGISTERED
        assertTrue(ManagementSystem.awaitRelationIndexStatus(graph, "livesByReason", "lives")
                .status(SchemaStatus.REGISTERED).call().getSucceeded());

        m = graph.openManagement();
        RelationTypeIndex index = m.getRelationIndex(m.getRelationType("lives"), "livesByReason");
        m.updateIndex(index, SchemaAction.ENABLE_INDEX);
        m.commit();
        graph.tx().commit();

        // Block until the SchemaStatus transitions to ENABLED
        assertTrue(ManagementSystem.awaitRelationIndexStatus(graph, "livesByReason", "lives")
                .status(SchemaStatus.ENABLED).call().getSucceeded());

        // Run a query that hits the index but erroneously returns nothing because we haven't repaired yet
        //assertFalse(graph.query().has("reason", "no fear of death").edges().iterator().hasNext());

        // Repair
        MapReduceIndexManagement mri = new MapReduceIndexManagement(graph);
        m = graph.openManagement();
        index = m.getRelationIndex(m.getRelationType("lives"), "livesByReason");
        ScanMetrics metrics = mri.updateIndex(index, SchemaAction.REINDEX).get();
        assertEquals(8, metrics.getCustom(IndexRepairJob.ADDED_RECORDS_COUNT));
    }
}
