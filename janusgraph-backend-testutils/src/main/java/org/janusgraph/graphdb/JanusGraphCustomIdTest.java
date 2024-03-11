// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.graphdb;

import com.google.common.base.Preconditions;
import io.github.artsok.RepeatedIfExceptionsTest;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraphConfigurationException;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.RelationTypeIndex;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.core.util.ManagementUtil;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.diskstorage.log.kcvs.KCVSLog;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.olap.job.GhostVertexRemover;
import org.janusgraph.graphdb.olap.job.IndexRemoveJob;
import org.janusgraph.graphdb.olap.job.IndexRepairJob;
import org.janusgraph.olap.OLAPTest;
import org.janusgraph.testutil.TestGraphConfigs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.process.traversal.Order.desc;
import static org.apache.tinkerpop.gremlin.structure.Direction.OUT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ALLOW_CUSTOM_VERTEX_ID_TYPES;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ALLOW_SETTING_VERTEX_ID;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_READ_INTERVAL;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_SEND_DELAY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.MANAGEMENT_LOG;
import static org.janusgraph.graphdb.internal.RelationCategory.EDGE;
import static org.janusgraph.graphdb.internal.RelationCategory.PROPERTY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This test suite is responsible for testing custom vertex id assignment
 */
public abstract class JanusGraphCustomIdTest extends JanusGraphBaseTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return getModifiableConfiguration().getConfiguration();
    }

    protected abstract ModifiableConfiguration getModifiableConfiguration();

    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        this.testInfo = testInfo;
        this.config = getConfiguration();
        TestGraphConfigs.applyOverrides(config);
        Preconditions.checkNotNull(config);
        logManagers = new HashMap<>();
        clearGraph(config);
        readConfig = new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS, config, BasicConfiguration.Restriction.NONE);
    }

    private void open(Boolean allowSettingVertexId, Boolean allowCustomVertexIdType) {
        ModifiableConfiguration config = getModifiableConfiguration();
        if (allowSettingVertexId != null) {
            config.set(ALLOW_SETTING_VERTEX_ID, allowSettingVertexId);
        }
        if (allowCustomVertexIdType != null) {
            config.set(ALLOW_CUSTOM_VERTEX_ID_TYPES, allowCustomVertexIdType);
        }
        open(config.getConfiguration());
    }

    @Test
    public void testConfig() {
        Exception ex = assertThrows(JanusGraphConfigurationException.class, () -> open(false, true));
        assertEquals("allow-custom-vid-types is enabled but set-vertex-id is disabled", ex.getMessage());
    }

    @Test
    public void testBasic() {
        open(true, true);
        Exception ex = assertThrows(IllegalArgumentException.class, () -> graph.addVertex());
        assertEquals("Must provide vertex id", ex.getMessage());
        ex = assertThrows(IllegalArgumentException.class, () -> graph.addVertex(T.id, "id™"));
        assertEquals("Custom string id contains non-ascii or non-printable character: id™", ex.getMessage());
        ex = assertThrows(IllegalArgumentException.class, () -> graph.addVertex(T.id, "custom-vertex"));
        assertEquals("Custom string id contains reserved string (-): custom-vertex", ex.getMessage());
        List<String> vids = new ArrayList<>();
        for (int i = 1; i < 100; i++) {
            StringBuilder builder = new StringBuilder();
            for (int j = 0; j < i; j++) {
                builder.append((char) ('a' + (j % 26)));
            }
            final String vid = builder.toString();
            vids.add(vid);
            graph.addVertex(T.id, vid, T.label, "person", "age", i);
        }
        graph.tx().commit();
        List<Object> resultIds = graph.traversal().V().id().toList();
        assertEquals(new HashSet<>(vids), new HashSet<>(resultIds));
    }

    /**
     * Test enabling custom string ID on an existing graph
     * instance which did not use custom string ID before
     */
    @Test
    public void testUpgrade() {
        open(false, false);
        // create some vertices
        graph.traversal().addV().property("prop", "val").next();
        graph.tx().commit();
        graph.close();

        // enable custom string ID mode
        open(null, null);
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.set(ConfigElement.getPath(ALLOW_SETTING_VERTEX_ID), true);
        mgmt.set(ConfigElement.getPath(ALLOW_CUSTOM_VERTEX_ID_TYPES), true);
        mgmt.commit();

        // open the graph again, it now has the latest config
        open(null, null);
        // can load the old vertices
        assertEquals(1, graph.traversal().V().count().next());
        // can create new vertices with string id
        graph.traversal().addV().property(T.id, "custom_id_1").property("prop", "val").next();
        // must provide vertex id now
        assertThrows(Exception.class, () -> graph.traversal().addV().next());
        // can create new vertices with long id
        graph.traversal().addV().property(T.id, graph.getIDManager().toVertexId(123L)).property("prop", "val").next();
        graph.tx().commit();
        assertEquals(3, graph.traversal().V().toList().size());
        assertEquals(3, graph.traversal().V().has("prop", "val").count().next());
    }

    /**
     * This test verifies that users could "downgrade"
     * in the sense that they could disable string id
     * feature after using it for a while
     */
    @Test
    public void testEnableAndDisableStringId() {
        open(true, true);
        graph.addVertex(T.id, "s_vid_a");
        graph.tx().commit();
        graph.close();

        // turn off custom string ID setting
        open(null, null);
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.set(ConfigElement.getPath(ALLOW_CUSTOM_VERTEX_ID_TYPES), false);
        mgmt.commit();

        open(null, null);
        Exception ex = assertThrows(UnsupportedOperationException.class, () -> graph.addVertex(T.id, "s_vid_b"));
        assertEquals("Vertex does not support user supplied identifiers of this type", ex.getMessage());
        graph.addVertex(T.id, graph.getIDManager().toVertexId(1L));
        graph.tx().commit();
        graph.close();

        // further turn off custom ID setting
        open(null, null);
        mgmt = graph.openManagement();
        mgmt.set(ConfigElement.getPath(ALLOW_SETTING_VERTEX_ID), false);
        mgmt.commit();

        open(null, null);
        graph.addVertex();
        graph.tx().commit();
        assertEquals(3, graph.traversal().V().count().next());
    }

    @Test
    public void testInvalidCustomLongVertexId() {
        open(true, true);
        // long ID must be converted first
        Exception ex = assertThrows(IllegalArgumentException.class, () -> graph.addVertex(T.id, 1));
        assertEquals("Not a valid vertex id: 1", ex.getMessage());
        graph.addVertex(T.id, graph.getIDManager().toVertexId(1));
        assertFalse(graph.traversal().V().hasId(1).hasNext());
        assertTrue(graph.traversal().V().hasId(graph.getIDManager().toVertexId(1)).hasNext());
        assertEquals(1, graph.getIDManager().fromVertexId((long) graph.traversal().V().id().next()));
        // string ID can be of any value
        graph.addVertex(T.id, "1");
        assertTrue(graph.traversal().V().hasId("1").hasNext());
    }

    /**
     * This is to test that JanusGraph properly handles vertex Ids that are of length
     * 7 and 8 (byte size of long type). See https://github.com/JanusGraph/janusgraph/issues/3732
     * to understand why these numbers are special.
     */
    @Test
    public void testSpecialLengthHandling() {
        open(true, true);
        // seven letters
        graph.traversal().addV().property(T.id, "abcdefg").next();
        assertTrue(graph.traversal().V("abcdefg").hasNext());

        // eight letters
        graph.traversal().addV().property(T.id, "abcdefgh").next();
        assertTrue(graph.traversal().V("abcdefgh").hasNext());

        graph.tx().commit();
        assertTrue(graph.traversal().V("abcdefg").hasNext());
        assertTrue(graph.traversal().V("abcdefgh").hasNext());
    }

    @Test
    public void testMixedStringAndLongVertexId() {
        open(true, true);
        List<Object> vids = new ArrayList<>();
        for (int i = 1; i < 100; i++) {
            Object vid;

            // add string-type ID
            StringBuilder builder = new StringBuilder();
            for (int j = 0; j < i; j++) {
                builder.append((char) ('a' + (j % 26)));
            }
            vid = builder.toString();
            assertEquals(i, vid.toString().length());
            vids.add(vid);
            graph.addVertex(T.id, vid);

            // add long-type ID
            vid = graph.getIDManager().toVertexId(i);
            vids.add(vid);
            graph.addVertex(T.id, vid);
        }
        graph.tx().commit();
        List<Object> resultIds = graph.traversal().V().id().toList();
        assertEquals(new HashSet<>(vids), new HashSet<>(resultIds));
    }

    @Test
    public void testBasicIndexLookUp() {
        open(true, true);
        final PropertyKey key = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex("nameKey", Vertex.class).addKey(key).buildCompositeIndex();
        finishSchema();

        tx.addVertex(T.id, "vid_alice", "name", "alice");
        tx.addVertex(T.id, "vid_bob", "name", "bob");
        newTx();

        assertEquals("vid_alice", graph.traversal().V().has("name", "alice").next().id());
        assertEquals("vid_alice", graph.traversal().V().has("name", "alice").id().next());
        assertEquals("alice", graph.traversal().V().has("name", "alice").values("name").next());
        assertEquals("alice", graph.traversal().V().has("name", "alice").next().value("name"));
    }

    /**
     * See {@link JanusGraphTest#testIndexUpdatesWithReindexAndRemove()}
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    // flaky test: https://github.com/JanusGraph/janusgraph/issues/1498
    @RepeatedIfExceptionsTest(repeats = 3)
    public void testIndexUpdatesWithReindexAndRemove() throws ExecutionException, InterruptedException {
        ModifiableConfiguration config = getModifiableConfiguration();
        config.set(ALLOW_SETTING_VERTEX_ID, true, new String[0]);
        config.set(ALLOW_CUSTOM_VERTEX_ID_TYPES, true, new String[0]);
        config.set(LOG_SEND_DELAY, Duration.ofMillis(0), MANAGEMENT_LOG);
        config.set(KCVSLog.LOG_READ_LAG_TIME, Duration.ofMillis(50), MANAGEMENT_LOG);
        config.set(LOG_READ_INTERVAL, Duration.ofMillis(250), MANAGEMENT_LOG);
        open(config.getConfiguration());

        //Types without index
        PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).make();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SET).make();
        EdgeLabel friend = mgmt.makeEdgeLabel("friend").multiplicity(Multiplicity.MULTI).make();
        PropertyKey sensor = mgmt.makePropertyKey("sensor").dataType(Double.class).cardinality(Cardinality.LIST).make();
        finishSchema();

        //Add some sensor & friend data
        JanusGraphVertex v = tx.addVertex(T.id, "first_vertex");
        for (int i = 0; i < 10; i++) {
            v.property("sensor", i, "time", i);
            v.property("name", "v" + i);
            JanusGraphVertex o = tx.addVertex(T.id, "vertex_" + i);
            v.addEdge("friend", o, "time", i);
        }
        newTx();
        //Indexes should not yet be in use
        v = getV(tx, v);
        evaluateQuery(v.query().keys("sensor").interval("time", 1, 5).orderBy("time", desc),
            PROPERTY, 4, 1, new boolean[]{false, false}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().keys("sensor").interval("time", 101, 105).orderBy("time", desc),
            PROPERTY, 0, 1, new boolean[]{false, false}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 1, 5).orderBy("time", desc),
            EDGE, 4, 1, new boolean[]{false, false}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 101, 105).orderBy("time", desc),
            EDGE, 0, 1, new boolean[]{false, false}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(tx.query().has("name", "v5"),
            ElementCategory.VERTEX, 1, new boolean[]{false, true});
        evaluateQuery(tx.query().has("name", "v105"),
            ElementCategory.VERTEX, 0, new boolean[]{false, true});
        newTx();

        //Create indexes after the fact
        finishSchema();
        sensor = mgmt.getPropertyKey("sensor");
        time = mgmt.getPropertyKey("time");
        name = mgmt.getPropertyKey("name");
        friend = mgmt.getEdgeLabel("friend");
        mgmt.buildPropertyIndex(sensor, "byTime", desc, time);
        mgmt.buildEdgeIndex(friend, "byTime", Direction.OUT, desc, time);
        mgmt.buildIndex("bySensorReading", Vertex.class).addKey(name).buildCompositeIndex();
        finishSchema();
        newTx();
        //Add some sensor & friend data that should already be indexed even though index is not yet enabled
        v = getV(tx, v);
        for (int i = 100; i < 110; i++) {
            v.property("sensor", i, "time", i);
            v.property("name", "v" + i);
            JanusGraphVertex o = tx.addVertex(T.id, "vertex_" + i);
            v.addEdge("friend", o, "time", i);
        }
        tx.commit();
        //Should not yet be able to enable since not yet registered
        final RelationTypeIndex pindex = mgmt.getRelationIndex(mgmt.getRelationType("sensor"), "byTime");
        final RelationTypeIndex eindex = mgmt.getRelationIndex(mgmt.getRelationType("friend"), "byTime");
        final JanusGraphIndex graphIndex = mgmt.getGraphIndex("bySensorReading");
        assertThrows(IllegalArgumentException.class, () -> mgmt.updateIndex(pindex, SchemaAction.ENABLE_INDEX));
        assertThrows(IllegalArgumentException.class, () -> mgmt.updateIndex(eindex, SchemaAction.ENABLE_INDEX));
        assertThrows(IllegalArgumentException.class, () -> mgmt.updateIndex(graphIndex, SchemaAction.ENABLE_INDEX));
        mgmt.commit();


        ManagementUtil.awaitVertexIndexUpdate(graph, "byTime", "sensor", 10, ChronoUnit.SECONDS);
        ManagementUtil.awaitGraphIndexUpdate(graph, "bySensorReading", 5, ChronoUnit.SECONDS);

        finishSchema();
        //Verify new status
        RelationTypeIndex pindex2 = mgmt.getRelationIndex(mgmt.getRelationType("sensor"), "byTime");
        RelationTypeIndex eindex2 = mgmt.getRelationIndex(mgmt.getRelationType("friend"), "byTime");
        JanusGraphIndex graphIndex2 = mgmt.getGraphIndex("bySensorReading");
        assertEquals(SchemaStatus.REGISTERED, pindex2.getIndexStatus());
        assertEquals(SchemaStatus.REGISTERED, eindex2.getIndexStatus());
        assertEquals(SchemaStatus.REGISTERED, graphIndex2.getIndexStatus(graphIndex2.getFieldKeys()[0]));
        finishSchema();
        //Simply enable without reindex
        eindex2 = mgmt.getRelationIndex(mgmt.getRelationType("friend"), "byTime");
        mgmt.updateIndex(eindex2, SchemaAction.ENABLE_INDEX);
        finishSchema();
        assertTrue(ManagementSystem.awaitRelationIndexStatus(graph, "byTime", "friend").status(SchemaStatus.ENABLED)
            .timeout(10L, ChronoUnit.SECONDS).call().getSucceeded());

        //Reindex the other two
        pindex2 = mgmt.getRelationIndex(mgmt.getRelationType("sensor"), "byTime");
        ScanMetrics reindexSensorByTime = mgmt.updateIndex(pindex2, SchemaAction.REINDEX).get();
        finishSchema();
        graphIndex2 = mgmt.getGraphIndex("bySensorReading");
        ScanMetrics reindexBySensorReading = mgmt.updateIndex(graphIndex2, SchemaAction.REINDEX).get();
        finishSchema();

        assertNotEquals(0, reindexSensorByTime.getCustom(IndexRepairJob.ADDED_RECORDS_COUNT));
        assertNotEquals(0, reindexBySensorReading.getCustom(IndexRepairJob.ADDED_RECORDS_COUNT));

        //Every index should now be enabled
        pindex2 = mgmt.getRelationIndex(mgmt.getRelationType("sensor"), "byTime");
        eindex2 = mgmt.getRelationIndex(mgmt.getRelationType("friend"), "byTime");
        graphIndex2 = mgmt.getGraphIndex("bySensorReading");
        assertEquals(SchemaStatus.ENABLED, eindex2.getIndexStatus());
        assertEquals(SchemaStatus.ENABLED, pindex2.getIndexStatus());
        assertEquals(SchemaStatus.ENABLED, graphIndex2.getIndexStatus(graphIndex2.getFieldKeys()[0]));


        //Add some more sensor & friend data
        newTx();
        v = getV(tx, v);
        for (int i = 200; i < 210; i++) {
            v.property("sensor", i, "time", i);
            v.property("name", "v" + i);
            JanusGraphVertex o = tx.addVertex(T.id, "vertex_" + i);
            v.addEdge("friend", o, "time", i);
        }
        newTx();
        //Use indexes now but only see new data for property and graph index
        v = getV(tx, v);
        evaluateQuery(v.query().keys("sensor").interval("time", 1, 5).orderBy("time", desc),
            PROPERTY, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().keys("sensor").interval("time", 101, 105).orderBy("time", desc),
            PROPERTY, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().keys("sensor").interval("time", 201, 205).orderBy("time", desc),
            PROPERTY, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 1, 5).orderBy("time", desc),
            EDGE, 0, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 101, 105).orderBy("time", desc),
            EDGE, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 201, 205).orderBy("time", desc),
            EDGE, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(tx.query().has("name", "v5"),
            ElementCategory.VERTEX, 1, new boolean[]{true, true}, "bySensorReading");
        evaluateQuery(tx.query().has("name", "v105"),
            ElementCategory.VERTEX, 1, new boolean[]{true, true}, "bySensorReading");
        evaluateQuery(tx.query().has("name", "v205"),
            ElementCategory.VERTEX, 1, new boolean[]{true, true}, "bySensorReading");

        finishSchema();
        eindex2 = mgmt.getRelationIndex(mgmt.getRelationType("friend"), "byTime");
        ScanMetrics reindexFriendByTime = mgmt.updateIndex(eindex2, SchemaAction.REINDEX).get();
        finishSchema();
        assertNotEquals(0, reindexFriendByTime.getCustom(IndexRepairJob.ADDED_RECORDS_COUNT));

        finishSchema();
        newTx();
        //It should now have all the answers
        v = getV(tx, v);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 1, 5).orderBy("time", desc),
            EDGE, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 101, 105).orderBy("time", desc),
            EDGE, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 201, 205).orderBy("time", desc),
            EDGE, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);

        pindex2 = mgmt.getRelationIndex(mgmt.getRelationType("sensor"), "byTime");
        graphIndex2 = mgmt.getGraphIndex("bySensorReading");
        mgmt.updateIndex(pindex2, SchemaAction.DISABLE_INDEX);
        mgmt.updateIndex(graphIndex2, SchemaAction.DISABLE_INDEX);
        mgmt.commit();
        tx.commit();

        ManagementUtil.awaitVertexIndexUpdate(graph, "byTime", "sensor", 10, ChronoUnit.SECONDS);
        ManagementUtil.awaitGraphIndexUpdate(graph, "bySensorReading", 5, ChronoUnit.SECONDS);

        finishSchema();

        pindex2 = mgmt.getRelationIndex(mgmt.getRelationType("sensor"), "byTime");
        graphIndex2 = mgmt.getGraphIndex("bySensorReading");
        assertEquals(SchemaStatus.DISABLED, pindex2.getIndexStatus());
        assertEquals(SchemaStatus.DISABLED, graphIndex2.getIndexStatus(graphIndex2.getFieldKeys()[0]));
        finishSchema();

        newTx();
        //The two disabled indexes should force full scans
        v = getV(tx, v);
        evaluateQuery(v.query().keys("sensor").interval("time", 1, 5).orderBy("time", desc),
            PROPERTY, 4, 1, new boolean[]{false, false}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().keys("sensor").interval("time", 101, 105).orderBy("time", desc),
            PROPERTY, 4, 1, new boolean[]{false, false}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().keys("sensor").interval("time", 201, 205).orderBy("time", desc),
            PROPERTY, 4, 1, new boolean[]{false, false}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 1, 5).orderBy("time", desc),
            EDGE, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 101, 105).orderBy("time", desc),
            EDGE, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 201, 205).orderBy("time", desc),
            EDGE, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(tx.query().has("name", "v5"),
            ElementCategory.VERTEX, 1, new boolean[]{false, true});
        evaluateQuery(tx.query().has("name", "v105"),
            ElementCategory.VERTEX, 1, new boolean[]{false, true});
        evaluateQuery(tx.query().has("name", "v205"),
            ElementCategory.VERTEX, 1, new boolean[]{false, true});

        tx.commit();
        finishSchema();
        pindex2 = mgmt.getRelationIndex(mgmt.getRelationType("sensor"), "byTime");
        graphIndex2 = mgmt.getGraphIndex("bySensorReading");
        ScanMetrics pmetrics = mgmt.updateIndex(pindex2, SchemaAction.DISCARD_INDEX).get();
        ScanMetrics graphIndexMetrics = mgmt.updateIndex(graphIndex2, SchemaAction.DISCARD_INDEX).get();
        finishSchema();
        assertEquals(30, pmetrics.getCustom(IndexRemoveJob.DELETED_RECORDS_COUNT));
        assertEquals(30, graphIndexMetrics.getCustom(IndexRemoveJob.DELETED_RECORDS_COUNT));
    }

    /**
     * See {@link OLAPTest#removeGhostVertices()}
     *
     * @throws Exception
     */
    @Test
    public void removeGhostVertices() throws Exception {
        open(true, true);
        JanusGraphVertex v1 = tx.addVertex(T.label, "person", T.id, "person1");
        v1.property("name", "stephen");
        JanusGraphVertex v2 = tx.addVertex(T.label, "person", T.id, "person2");
        v1.property("name", "marko");
        JanusGraphVertex v3 = tx.addVertex(T.label, "person", T.id, graph.getIDManager().toVertexId(3));
        v1.property("name", "dan");
        v2.addEdge("knows", v3);
        v1.addEdge("knows", v2);
        newTx();
        Object v3id = getId(v3);
        Object v1id = getId(v1);
        assertTrue(v1id instanceof String);
        assertTrue((long) v3id > 0);

        v3 = getV(tx, v3id);
        assertNotNull(v3);
        v3.remove();
        tx.commit();

        JanusGraphTransaction xx = graph.buildTransaction().checkExternalVertexExistence(false).start();
        v3 = getV(xx, v3id);
        assertNotNull(v3);
        v1 = getV(xx, v1id);
        assertNotNull(v1);
        v3.property("name", "deleted");
        v3.addEdge("knows", v1);
        xx.commit();

        newTx();
        assertNull(getV(tx, v3id));
        v1 = getV(tx, v1id);
        assertNotNull(v1);
        assertEquals(v3id, v1.query().direction(Direction.IN).labels("knows").vertices().iterator().next().id());
        tx.commit();
        mgmt.commit();

        ScanMetrics result = executeScanJob(new GhostVertexRemover(graph));
        assertEquals(1, result.getCustom(GhostVertexRemover.REMOVED_VERTEX_COUNT));
        assertEquals(2, result.getCustom(GhostVertexRemover.REMOVED_RELATION_COUNT));
        assertEquals(0, result.getCustom(GhostVertexRemover.SKIPPED_GHOST_LIMIT_COUNT));

        // Second scan should not find any ghost vertices
        result = executeScanJob(new GhostVertexRemover(graph));
        assertEquals(0, result.getCustom(GhostVertexRemover.REMOVED_VERTEX_COUNT));
        assertEquals(0, result.getCustom(GhostVertexRemover.REMOVED_RELATION_COUNT));
        assertEquals(0, result.getCustom(GhostVertexRemover.SKIPPED_GHOST_LIMIT_COUNT));
    }

    @Test
    public void testWriteAndReadWithJanusGraphIoRegistryWithGryo(@TempDir Path tempDir) {
        open(true, true);
        final Path file = tempDir.resolve("testgraph_" + this.getClass().getCanonicalName() + ".kryo");
        testWritingAndReading(file.toFile());
    }

    @Test
    public void testWriteAndReadWithJanusGraphIoRegistryWithGraphson(@TempDir Path tempDir) {
        open(true, true);
        final Path file = tempDir.resolve("testgraph_" + this.getClass().getCanonicalName() + ".json");
        testWritingAndReading(file.toFile());
    }

    private void testWritingAndReading(File f) {
        GraphTraversalSource g = graph.traversal();
        Vertex fromV = g.addV().property("name", f.getName()).property(T.id, "custom_id").next();
        Vertex toV = g.addV().property(T.id, "another_vertex").next();
        g.addE("connect").from(fromV).to(toV).next();
        g.tx().commit();
        assertEquals(0, f.length());

        g.io(f.getAbsolutePath()).write().iterate();

        assertTrue(f.length() > 0, "File " + f.getAbsolutePath() + " was expected to be not empty, but is");

        open(true, true);
        g = graph.traversal();
        g.V().drop().iterate();
        g.tx().commit();

        g.io(f.getAbsolutePath()).read().iterate();

        assertEquals(1, g.V().has("name", f.getName()).count().next());
        assertEquals(1, g.V("custom_id").count().next());
        assertEquals(2, g.V().count().next());
        assertEquals("connect", g.E().label().next());
    }

}
