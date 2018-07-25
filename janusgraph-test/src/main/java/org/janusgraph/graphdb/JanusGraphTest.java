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

package org.janusgraph.graphdb;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.SchemaViolationException;
import org.janusgraph.core.JanusGraphConfigurationException;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphQuery;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.JanusGraphVertexQuery;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.VertexList;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Contain;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.log.Change;
import org.janusgraph.core.log.ChangeProcessor;
import org.janusgraph.core.log.ChangeState;
import org.janusgraph.core.log.LogProcessorFramework;
import org.janusgraph.core.log.TransactionId;
import org.janusgraph.core.log.TransactionRecovery;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.RelationTypeIndex;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.JanusGraphSchemaType;
import org.janusgraph.core.util.JanusGraphCleanup;
import org.janusgraph.core.util.ManagementUtil;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.diskstorage.log.Log;
import org.janusgraph.diskstorage.log.Message;
import org.janusgraph.diskstorage.log.MessageReader;
import org.janusgraph.diskstorage.log.ReadMarker;
import org.janusgraph.diskstorage.log.kcvs.KCVSLog;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.EdgeSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.log.LogTxMeta;
import org.janusgraph.graphdb.database.log.LogTxStatus;
import org.janusgraph.graphdb.database.log.TransactionLogHeader;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.internal.RelationCategory;
import org.janusgraph.graphdb.log.StandardTransactionLogProcessor;
import org.janusgraph.graphdb.olap.job.IndexRemoveJob;
import org.janusgraph.graphdb.olap.job.IndexRepairJob;
import org.janusgraph.graphdb.query.graph.GraphCentricQueryBuilder;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.query.profile.SimpleQueryProfiler;
import org.janusgraph.graphdb.query.vertex.BasicVertexCentricQueryBuilder;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.schema.EdgeLabelDefinition;
import org.janusgraph.graphdb.schema.PropertyKeyDefinition;
import org.janusgraph.graphdb.schema.SchemaContainer;
import org.janusgraph.graphdb.schema.VertexLabelDefinition;
import org.janusgraph.graphdb.serializer.SpecialInt;
import org.janusgraph.graphdb.serializer.SpecialIntSerializer;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphStep;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphPropertiesStep;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphVertexStep;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.StandardEdgeLabelMaker;
import org.janusgraph.graphdb.types.StandardPropertyKeyMaker;
import org.janusgraph.graphdb.types.system.BaseVertexLabel;
import org.janusgraph.graphdb.types.system.ImplicitKey;
import org.janusgraph.testcategory.BrittleTests;
import org.janusgraph.testutil.TestGraphConfigs;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;
import static org.janusgraph.graphdb.internal.RelationCategory.*;
import static org.janusgraph.testutil.JanusGraphAssert.*;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.decr;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.incr;
import static org.apache.tinkerpop.gremlin.structure.Direction.*;
import static org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.single;
import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class JanusGraphTest extends JanusGraphBaseTest {

    private Logger log = LoggerFactory.getLogger(JanusGraphTest.class);

    final boolean isLockingOptimistic() {
        return features.hasOptimisticLocking();
    }

  /* ==================================================================================
                            INDEXING
     ==================================================================================*/

    /**
     * Just opens and closes the graph
     */
    @Test
    public void testOpenClose() {
    }

    /**
     * Ensure clearing storage actually removes underlying database.
     * @throws Exception
     */
    @Test
    public void testClearStorage() throws Exception {
        tearDown();
        config.set(ConfigElement.getPath(GraphDatabaseConfiguration.DROP_ON_CLEAR), true);
        final Backend backend = getBackend(config, false);
        assertTrue("graph should exist before clearing storage", backend.getStoreManager().exists());
        clearGraph(config);
        assertFalse("graph should not exist after clearing storage", backend.getStoreManager().exists());
    }

    /**
     * Very simple graph operation to ensure minimal functionality and cleanup
     */
    @Test
    public void testBasic() {

        PropertyKey uid = makeVertexIndexedUniqueKey("name", String.class);
        finishSchema();

        JanusGraphVertex n1 = tx.addVertex();
        uid = tx.getPropertyKey("name");
        n1.property(uid.name(), "abcd");
        clopen();
        long nid = n1.longId();
        uid = tx.getPropertyKey("name");
        assertTrue(getV(tx, nid) != null);
        assertTrue(getV(tx, uid.longId()) != null);
        assertMissing(tx, nid + 64);
        uid = tx.getPropertyKey(uid.name());
        n1 = getV(tx, nid);
        assertEquals(n1, getOnlyVertex(tx.query().has(uid.name(), "abcd")));
        assertEquals(1, Iterables.size(n1.query().relations())); //TODO: how to expose relations?
        assertEquals("abcd", n1.value(uid.name()));
        assertCount(1, tx.query().vertices());
        close();
        JanusGraphCleanup.clear(graph);
        open(config);
        assertEmpty(tx.query().vertices());
    }

    /**
     * Adding a removing a vertex with index
     */
    @Test
    public void testVertexRemoval() {
        final String namen = "name";
        makeVertexIndexedUniqueKey(namen, String.class);
        finishSchema();

        JanusGraphVertex v1 = graph.addVertex(namen, "v1");
        JanusGraphVertex v2 = graph.addVertex(namen, "v2");
        v1.addEdge("knows", v2);
        assertCount(2, graph.query().vertices());
        assertCount(1, graph.query().has(namen, "v2").vertices());

        clopen();

        v1 = getV(graph, v1);
        v2 = getV(graph, v2);
        assertCount(1, v1.query().direction(BOTH).edges());
        assertCount(1, v2.query().direction(Direction.BOTH).edges());
        v2.remove();
        assertCount(0, v1.query().direction(Direction.BOTH).edges());
        try {
            assertCount(0, v2.query().direction(Direction.BOTH).edges());
            fail();
        } catch (IllegalStateException ex) {
        }
        assertCount(1, graph.query().vertices());
        assertCount(1, graph.query().has(namen, "v1").vertices());
        assertCount(0, graph.query().has(namen, "v2").vertices());
        graph.tx().commit();

        assertMissing(graph, v2);
        assertCount(1, graph.query().vertices());
        assertCount(1, graph.query().has(namen, "v1").vertices());
        assertCount(0, graph.query().has(namen, "v2").vertices());
    }

    /**
     * Iterating over all vertices and edges in a graph
     */
    @Test
    public void testGlobalIteration() {
        int numV = 50;
        int deleteV = 5;

        JanusGraphVertex previous = tx.addVertex("count", 0);
        for (int i = 1; i < numV; i++) {
            JanusGraphVertex next = tx.addVertex("count", i);
            previous.addEdge("next", next);
            previous = next;
        }
        int numE = numV - 1;
        assertCount(numV, tx.query().vertices());
        assertCount(numV, tx.query().vertices());
        assertCount(numE, tx.query().edges());
        assertCount(numE, tx.query().edges());

        clopen();

        assertCount(numV, tx.query().vertices());
        assertCount(numV, tx.query().vertices());
        assertCount(numE, tx.query().edges());
        assertCount(numE, tx.query().edges());

        //tx.V().range(0,deleteV).remove();
        for (Object v : tx.query().limit(deleteV).vertices()) {
            ((JanusGraphVertex) v).remove();
        }

        for (int i = 0; i < 10; i++) { //Repeated vertex counts
            assertCount(numV - deleteV, tx.query().vertices());
            assertCount(numV - deleteV, tx.query().has("count", Cmp.GREATER_THAN_EQUAL, 0).vertices());
        }

        clopen();
        for (int i = 0; i < 10; i++) { //Repeated vertex counts
            assertCount(numV - deleteV, tx.query().vertices());
            assertCount(numV - deleteV, tx.query().has("count", Cmp.GREATER_THAN_EQUAL, 0).vertices());
        }
    }

    @Test
    public void testMediumCreateRetrieve() {
        //Create schema
        makeLabel("connect");
        makeVertexIndexedUniqueKey("name", String.class);
        PropertyKey weight = makeKey("weight", Double.class);
        PropertyKey id = makeVertexIndexedUniqueKey("uid", Integer.class);
        ((StandardEdgeLabelMaker) mgmt.makeEdgeLabel("knows")).sortKey(id).signature(weight).make();
        finishSchema();

        //Create Nodes
        int noVertices = 500;
        String[] names = new String[noVertices];
        int[] ids = new int[noVertices];
        JanusGraphVertex[] nodes = new JanusGraphVertex[noVertices];
        long[] nodeIds = new long[noVertices];
        List<Edge>[] nodeEdges = new List[noVertices];
        for (int i = 0; i < noVertices; i++) {
            names[i] = "vertex" + i;
            ids[i] = i;
            nodes[i] = tx.addVertex("name", names[i], "uid", ids[i]);
            if ((i + 1) % 100 == 0) log.debug("Added 100 nodes");
        }
        log.debug("Nodes created");
        int[] connectOff = {-100, -34, -4, 10, 20};
        int[] knowsOff = {-400, -18, 8, 232, 334};
        for (int i = 0; i < noVertices; i++) {
            JanusGraphVertex n = nodes[i];
            nodeEdges[i] = new ArrayList<Edge>(10);
            for (int c : connectOff) {
                Edge r = n.addEdge("connect", nodes[wrapAround(i + c, noVertices)]);
                nodeEdges[i].add(r);
            }
            for (int k : knowsOff) {
                JanusGraphVertex n2 = nodes[wrapAround(i + k, noVertices)];
                Edge r = n.addEdge("knows", n2,
                        "uid", ((Number) n.value("uid")).intValue() + ((Number) n2.value("uid")).intValue(),
                        "weight", k * 1.5,
                        "name", i + "-" + k);
                nodeEdges[i].add(r);
            }
            if (i % 100 == 99) log.debug(".");
        }

        tx.commit();
        tx = null;
        Set[] nodeEdgeIds = new Set[noVertices];
        for (int i = 0; i < noVertices; i++) {
            nodeIds[i] = (Long) nodes[i].id();
            nodeEdgeIds[i] = new HashSet(10);
            for (Object r : nodeEdges[i]) {
                nodeEdgeIds[i].add(((JanusGraphEdge) r).longId());
            }
        }
        clopen();

        nodes = new JanusGraphVertex[noVertices];
        for (int i = 0; i < noVertices; i++) {
            JanusGraphVertex n = getVertex("uid", ids[i]);
            assertEquals(n, getVertex("name", names[i]));
            assertEquals(names[i], n.value("name"));
            nodes[i] = n;
            assertEquals(nodeIds[i], n.longId());
        }
        for (int i = 0; i < noVertices; i++) {
            JanusGraphVertex n = nodes[i];
            assertCount(connectOff.length + knowsOff.length, n.query().direction(Direction.OUT).edges());
            assertCount(connectOff.length, n.query().direction(Direction.OUT).labels("connect").edges());
            assertCount(connectOff.length * 2, n.query().direction(Direction.BOTH).labels("connect").edges());
            assertCount(knowsOff.length * 2, n.query().direction(Direction.BOTH).labels("knows").edges());

            assertCount(connectOff.length + knowsOff.length, n.query().direction(Direction.OUT).edges());
            assertCount(2, n.properties());
            for (Object o : n.query().direction(Direction.OUT).labels("knows").edges()) {
                JanusGraphEdge r = (JanusGraphEdge) o;
                JanusGraphVertex n2 = r.vertex(Direction.IN);
                int idsum = ((Number) n.value("uid")).intValue() + ((Number) n2.value("uid")).intValue();
                assertEquals(idsum, ((Number) r.value("uid")).intValue());
                double k = ((Number) r.value("weight")).doubleValue() / 1.5;
                int ki = (int) k;
                assertEquals(i + "-" + ki, r.value("name"));
            }

            Set edgeIds = new HashSet(10);
            for (Object r : n.query().direction(Direction.OUT).edges()) {
                edgeIds.add(((JanusGraphEdge) r).longId());
            }
            assertTrue(edgeIds + " vs " + nodeEdgeIds[i], edgeIds.equals(nodeEdgeIds[i]));
        }
        newTx();
        //Bulk vertex retrieval
        long[] vids1 = new long[noVertices / 10];
        for (int i = 0; i < vids1.length; i++) {
            vids1[i] = nodeIds[i];
        }
        //All non-cached
        verifyVerticesRetrieval(vids1, Lists.newArrayList(tx.getVertices(vids1)));

        //All cached
        verifyVerticesRetrieval(vids1, Lists.newArrayList(tx.getVertices(vids1)));

        long[] vids2 = new long[noVertices / 10 * 2];
        for (int i = 0; i < vids2.length; i++) {
            vids2[i] = nodeIds[i];
        }
        //Partially cached
        verifyVerticesRetrieval(vids2, Lists.newArrayList(tx.getVertices(vids2)));
    }

    private void verifyVerticesRetrieval(long[] vids, List<JanusGraphVertex> vs) {
        assertEquals(vids.length, vs.size());
        Set<Long> vset = new HashSet<>(vs.size());
        vs.forEach(v -> vset.add((Long) v.id()));
        for (int i = 0; i < vids.length; i++) {
            assertTrue(vset.contains(vids[i]));
        }
    }


    /* ==================================================================================
                            SCHEMA TESTS
     ==================================================================================*/

    /**
     * Test the definition and inspection of various schema types and ensure their correct interpretation
     * within the graph
     */
    @SuppressWarnings("unchecked")
  @Test
    public void testSchemaTypes() {
        // ---------- PROPERTY KEYS ----------------
        //Normal single-valued property key
        PropertyKey weight = makeKey("weight", Float.class);
        //Indexed unique property key
        PropertyKey uid = makeVertexIndexedUniqueKey("uid", String.class);
        //Indexed but not unique
        PropertyKey someid = makeVertexIndexedKey("someid", Object.class);
        //Set-valued property key
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SET).make();
        //List-valued property key with signature
        PropertyKey value = mgmt.makePropertyKey("value").dataType(Double.class).signature(weight).cardinality(Cardinality.LIST).make();

        // ---------- EDGE LABELS ----------------
        //Standard edge label
        EdgeLabel friend = mgmt.makeEdgeLabel("friend").make();
        //Unidirected
        EdgeLabel link = mgmt.makeEdgeLabel("link").unidirected().multiplicity(Multiplicity.MANY2ONE).make();
        //Signature label
        EdgeLabel connect = mgmt.makeEdgeLabel("connect").signature(uid).multiplicity(Multiplicity.SIMPLE).make();
        //Edge labels with different cardinalities
        EdgeLabel parent = mgmt.makeEdgeLabel("parent").multiplicity(Multiplicity.MANY2ONE).make();
        EdgeLabel child = mgmt.makeEdgeLabel("child").multiplicity(Multiplicity.ONE2MANY).make();
        EdgeLabel spouse = mgmt.makeEdgeLabel("spouse").multiplicity(Multiplicity.ONE2ONE).make();

        // ---------- VERTEX LABELS ----------------

        VertexLabel person = mgmt.makeVertexLabel("person").make();
        VertexLabel tag = mgmt.makeVertexLabel("tag").make();
        VertexLabel tweet = mgmt.makeVertexLabel("tweet").setStatic().make();

        long[] sig;

        // ######### INSPECTION & FAILURE ############

        assertTrue(mgmt.isOpen());
        assertEquals("weight", weight.toString());
        assertTrue(mgmt.containsRelationType("weight"));
        assertTrue(mgmt.containsPropertyKey("weight"));
        assertFalse(mgmt.containsEdgeLabel("weight"));
        assertTrue(mgmt.containsEdgeLabel("connect"));
        assertFalse(mgmt.containsPropertyKey("connect"));
        assertFalse(mgmt.containsRelationType("bla"));
        assertNull(mgmt.getPropertyKey("bla"));
        assertNull(mgmt.getEdgeLabel("bla"));
        assertNotNull(mgmt.getPropertyKey("weight"));
        assertNotNull(mgmt.getEdgeLabel("connect"));
        assertTrue(weight.isPropertyKey());
        assertFalse(weight.isEdgeLabel());
        assertEquals(Cardinality.SINGLE, weight.cardinality());
        assertEquals(Cardinality.SINGLE, someid.cardinality());
        assertEquals(Cardinality.SET, name.cardinality());
        assertEquals(Cardinality.LIST, value.cardinality());
        assertEquals(Object.class, someid.dataType());
        assertEquals(Float.class, weight.dataType());
        sig = ((InternalRelationType) value).getSignature();
        assertEquals(1, sig.length);
        assertEquals(weight.longId(), sig[0]);
        assertTrue(mgmt.getGraphIndex(uid.name()).isUnique());
        assertFalse(mgmt.getGraphIndex(someid.name()).isUnique());

        assertEquals("friend", friend.name());
        assertTrue(friend.isEdgeLabel());
        assertFalse(friend.isPropertyKey());
        assertEquals(Multiplicity.ONE2ONE, spouse.multiplicity());
        assertEquals(Multiplicity.ONE2MANY, child.multiplicity());
        assertEquals(Multiplicity.MANY2ONE, parent.multiplicity());
        assertEquals(Multiplicity.MULTI, friend.multiplicity());
        assertEquals(Multiplicity.SIMPLE, connect.multiplicity());
        assertTrue(link.isUnidirected());
        assertFalse(link.isDirected());
        assertFalse(child.isUnidirected());
        assertTrue(spouse.isDirected());
        assertFalse(((InternalRelationType) friend).isInvisibleType());
        assertTrue(((InternalRelationType) friend).isInvisible());
        assertEquals(0, ((InternalRelationType) friend).getSignature().length);
        sig = ((InternalRelationType) connect).getSignature();
        assertEquals(1, sig.length);
        assertEquals(uid.longId(), sig[0]);
        assertEquals(0, ((InternalRelationType) friend).getSortKey().length);
        assertEquals(Order.DEFAULT, ((InternalRelationType) friend).getSortOrder());
        assertEquals(SchemaStatus.ENABLED, ((InternalRelationType) friend).getStatus());

        assertEquals(5, Iterables.size(mgmt.getRelationTypes(PropertyKey.class)));
        assertEquals(6, Iterables.size(mgmt.getRelationTypes(EdgeLabel.class)));
        assertEquals(11, Iterables.size(mgmt.getRelationTypes(RelationType.class)));
        assertEquals(3, Iterables.size(mgmt.getVertexLabels()));

        assertEquals("tweet", tweet.name());
        assertTrue(mgmt.containsVertexLabel("person"));
        assertFalse(mgmt.containsVertexLabel("bla"));
        assertFalse(person.isPartitioned());
        assertFalse(person.isStatic());
        assertFalse(tag.isPartitioned());
        assertTrue(tweet.isStatic());

        //------ TRY INVALID STUFF --------

        //Failures
        try {
            //No datatype
            mgmt.makePropertyKey("fid").make();
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //Already exists
            mgmt.makeEdgeLabel("link").unidirected().make();
            fail();
        } catch (SchemaViolationException e) {
        }
        try {
            //signature and sort-key collide
            ((StandardEdgeLabelMaker) mgmt.makeEdgeLabel("other")).
                    sortKey(someid, weight).signature(someid).make();
            fail();
        } catch (IllegalArgumentException e) {
        }
//        try {
//            //keys must be single-valued
//            ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("other")).
//                    sortKey(name, weight).make();
//            fail();
//        } catch (IllegalArgumentException e) {}
        try {
            //sort key requires the label to be non-constrained
            ((StandardEdgeLabelMaker) mgmt.makeEdgeLabel("other")).multiplicity(Multiplicity.SIMPLE).
                    sortKey(weight).make();
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //sort key requires the label to be non-constrained
            ((StandardEdgeLabelMaker) mgmt.makeEdgeLabel("other")).multiplicity(Multiplicity.MANY2ONE).
                    sortKey(weight).make();
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //Already exists
            mgmt.makeVertexLabel("tweet").make();
            fail();
        } catch (SchemaViolationException e) {
        }
        try {
            //signature key must have non-generic data type
            mgmt.makeEdgeLabel("test").signature(someid).make();
            fail();
        } catch (IllegalArgumentException e) {
        }

        // ######### END INSPECTION ############


        finishSchema();
        clopen();

        //Load schema types into current transaction
        weight = mgmt.getPropertyKey("weight");
        uid = mgmt.getPropertyKey("uid");
        someid = mgmt.getPropertyKey("someid");
        name = mgmt.getPropertyKey("name");
        value = mgmt.getPropertyKey("value");
        friend = mgmt.getEdgeLabel("friend");
        link = mgmt.getEdgeLabel("link");
        connect = mgmt.getEdgeLabel("connect");
        parent = mgmt.getEdgeLabel("parent");
        child = mgmt.getEdgeLabel("child");
        spouse = mgmt.getEdgeLabel("spouse");
        person = mgmt.getVertexLabel("person");
        tag = mgmt.getVertexLabel("tag");
        tweet = mgmt.getVertexLabel("tweet");


        // ######### INSPECTION & FAILURE (COPIED FROM ABOVE) ############

        assertTrue(mgmt.isOpen());
        assertEquals("weight", weight.toString());
        assertTrue(mgmt.containsRelationType("weight"));
        assertTrue(mgmt.containsPropertyKey("weight"));
        assertFalse(mgmt.containsEdgeLabel("weight"));
        assertTrue(mgmt.containsEdgeLabel("connect"));
        assertFalse(mgmt.containsPropertyKey("connect"));
        assertFalse(mgmt.containsRelationType("bla"));
        assertNull(mgmt.getPropertyKey("bla"));
        assertNull(mgmt.getEdgeLabel("bla"));
        assertNotNull(mgmt.getPropertyKey("weight"));
        assertNotNull(mgmt.getEdgeLabel("connect"));
        assertTrue(weight.isPropertyKey());
        assertFalse(weight.isEdgeLabel());
        assertEquals(Cardinality.SINGLE, weight.cardinality());
        assertEquals(Cardinality.SINGLE, someid.cardinality());
        assertEquals(Cardinality.SET, name.cardinality());
        assertEquals(Cardinality.LIST, value.cardinality());
        assertEquals(Object.class, someid.dataType());
        assertEquals(Float.class, weight.dataType());
        sig = ((InternalRelationType) value).getSignature();
        assertEquals(1, sig.length);
        assertEquals(weight.longId(), sig[0]);
        assertTrue(mgmt.getGraphIndex(uid.name()).isUnique());
        assertFalse(mgmt.getGraphIndex(someid.name()).isUnique());

        assertEquals("friend", friend.name());
        assertTrue(friend.isEdgeLabel());
        assertFalse(friend.isPropertyKey());
        assertEquals(Multiplicity.ONE2ONE, spouse.multiplicity());
        assertEquals(Multiplicity.ONE2MANY, child.multiplicity());
        assertEquals(Multiplicity.MANY2ONE, parent.multiplicity());
        assertEquals(Multiplicity.MULTI, friend.multiplicity());
        assertEquals(Multiplicity.SIMPLE, connect.multiplicity());
        assertTrue(link.isUnidirected());
        assertFalse(link.isDirected());
        assertFalse(child.isUnidirected());
        assertTrue(spouse.isDirected());
        assertFalse(((InternalRelationType) friend).isInvisibleType());
        assertTrue(((InternalRelationType) friend).isInvisible());
        assertEquals(0, ((InternalRelationType) friend).getSignature().length);
        sig = ((InternalRelationType) connect).getSignature();
        assertEquals(1, sig.length);
        assertEquals(uid.longId(), sig[0]);
        assertEquals(0, ((InternalRelationType) friend).getSortKey().length);
        assertEquals(Order.DEFAULT, ((InternalRelationType) friend).getSortOrder());
        assertEquals(SchemaStatus.ENABLED, ((InternalRelationType) friend).getStatus());

        assertEquals(5, Iterables.size(mgmt.getRelationTypes(PropertyKey.class)));
        assertEquals(6, Iterables.size(mgmt.getRelationTypes(EdgeLabel.class)));
        assertEquals(11, Iterables.size(mgmt.getRelationTypes(RelationType.class)));
        assertEquals(3, Iterables.size(mgmt.getVertexLabels()));

        assertEquals("tweet", tweet.name());
        assertTrue(mgmt.containsVertexLabel("person"));
        assertFalse(mgmt.containsVertexLabel("bla"));
        assertFalse(person.isPartitioned());
        assertFalse(person.isStatic());
        assertFalse(tag.isPartitioned());
        assertTrue(tweet.isStatic());

        //------ TRY INVALID STUFF --------

        //Failures
        try {
            //No datatype
            mgmt.makePropertyKey("fid").make();
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //Already exists
            mgmt.makeEdgeLabel("link").unidirected().make();
            fail();
        } catch (SchemaViolationException e) {
        }
        try {
            //signature and sort-key collide
            ((StandardEdgeLabelMaker) mgmt.makeEdgeLabel("other")).
                    sortKey(someid, weight).signature(someid).make();
            fail();
        } catch (IllegalArgumentException e) {
        }
//        try {
//            //keys must be single-valued
//            ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("other")).
//                    sortKey(name, weight).make();
//            fail();
//        } catch (IllegalArgumentException e) {}
        try {
            //sort key requires the label to be non-constrained
            ((StandardEdgeLabelMaker) mgmt.makeEdgeLabel("other")).multiplicity(Multiplicity.SIMPLE).
                    sortKey(weight).make();
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //sort key requires the label to be non-constrained
            ((StandardEdgeLabelMaker) mgmt.makeEdgeLabel("other")).multiplicity(Multiplicity.MANY2ONE).
                    sortKey(weight).make();
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //Already exists
            mgmt.makeVertexLabel("tweet").make();
            fail();
        } catch (SchemaViolationException e) {
        }
        try {
            //signature key must have non-generic data type
            mgmt.makeEdgeLabel("test").signature(someid).make();
            fail();
        } catch (IllegalArgumentException e) {
        }

        // ######### END INSPECTION ############

        /*
          ####### Make sure schema semantics are honored in transactions ######
        */

        clopen();

        JanusGraphTransaction tx2;
        assertEmpty(tx.query().has("uid", "v1").vertices()); //shouldn't exist

        JanusGraphVertex v = tx.addVertex();
        //test property keys
        v.property("uid", "v1");
        v.property("weight", 1.5);
        v.property("someid", "Hello");
        v.property("name", "Bob");
        v.property("name", "John");
        VertexProperty p = v.property("value", 11);
        p.property("weight", 22);
        v.property("value", 33.3, "weight", 66.6);
        v.property("value", 11, "weight", 22); //same values are supported for list-properties
        //test edges
        JanusGraphVertex v12 = tx.addVertex("person"), v13 = tx.addVertex("person");
        v12.property("uid", "v12");
        v13.property("uid", "v13");
        v12.addEdge("parent", v, "weight", 4.5);
        v13.addEdge("parent", v, "weight", 4.5);
        v.addEdge("child", v12);
        v.addEdge("child", v13);
        v.addEdge("spouse", v12);
        v.addEdge("friend", v12);
        v.addEdge("friend", v12); //supports multi edges
        v.addEdge("connect", v12, "uid", "e1");
        v.addEdge("link", v13);
        JanusGraphVertex v2 = tx.addVertex("tweet");
        v2.addEdge("link", v13);
        v12.addEdge("connect", v2);
        JanusGraphEdge edge;

        // ######### INSPECTION & FAILURE ############
        assertEquals(v, getOnlyElement(tx.query().has("uid", Cmp.EQUAL, "v1").vertices()));
        v = getOnlyVertex(tx.query().has("uid", Cmp.EQUAL, "v1"));
        v12 = getOnlyVertex(tx.query().has("uid", Cmp.EQUAL, "v12"));
        v13 = getOnlyVertex(tx.query().has("uid", Cmp.EQUAL, "v13"));
        try {
            //Invalid data type
            v.property("weight", "x");
            fail();
        } catch (SchemaViolationException e) {
        }
        try {
            //Only one "John" should be allowed
            v.property(VertexProperty.Cardinality.list, "name", "John");
            fail();
        } catch (SchemaViolationException e) {
        }
        try {
            //Cannot set a property as edge
            v.property("link", v);
            fail();
        } catch (IllegalArgumentException e) {
        }


        //Only one property for weight allowed
        v.property(single, "weight", 1.0);
        assertCount(1, v.properties("weight"));
        v.property(VertexProperty.Cardinality.single, "weight", 0.5);
        assertEquals(0.5, v.<Float>value("weight").doubleValue(), 0.00001);
        assertEquals("v1", v.value("uid"));
        assertCount(2, v.properties("name"));
        for (Object prop : v.query().labels("name").properties()) {
            String nstr = ((JanusGraphVertexProperty<String>) prop).value();
            assertTrue(nstr.equals("Bob") || nstr.equals("John"));
        }
        assertTrue(size(v.properties("value")) >= 3);
        for (Object o : v.query().labels("value").properties()) {
            JanusGraphVertexProperty<Double> prop = (JanusGraphVertexProperty<Double>) o;
            double prec = prop.value().doubleValue();
            assertEquals(prec * 2, prop.<Number>value("weight").doubleValue(), 0.00001);
        }
        //Ensure we can add additional values
        p = v.property("value", 44.4, "weight", 88.8);
        assertEquals(v, getOnlyElement(tx.query().has("someid", Cmp.EQUAL, "Hello").vertices()));

        //------- EDGES -------
        try {
            //multiplicity violation
            v12.addEdge("parent", v13);
            fail();
        } catch (SchemaViolationException e) {
        }
        try {
            //multiplicity violation
            v13.addEdge("child", v12);
            fail();
        } catch (SchemaViolationException e) {
        }
        try {
            //multiplicity violation
            v13.addEdge("spouse", v12);
            fail();
        } catch (SchemaViolationException e) {
        }
        try {
            //multiplicity violation
            v.addEdge("spouse", v13);
            fail();
        } catch (SchemaViolationException e) {
        }
        assertCount(2, v.query().direction(Direction.IN).labels("parent").edges());
        assertCount(1, v12.query().direction(Direction.OUT).labels("parent").has("weight").edges());
        assertCount(1, v13.query().direction(Direction.OUT).labels("parent").has("weight").edges());
        assertEquals(v12, getOnlyElement(v.query().direction(Direction.OUT).labels("spouse").vertices()));
        edge = Iterables.<JanusGraphEdge>getOnlyElement(v.query().direction(Direction.BOTH).labels("connect").edges());
        assertEquals(1, edge.keys().size());
        assertEquals("e1", edge.value("uid"));
        try {
            //connect is simple
            v.addEdge("connect", v12);
            fail();
        } catch (SchemaViolationException e) {
        }
        //Make sure "link" is unidirected
        assertCount(1, v.query().direction(Direction.BOTH).labels("link").edges());
        assertCount(0, v13.query().direction(Direction.BOTH).labels("link").edges());
        //Assert we can add more friendships
        v.addEdge("friend", v12);
        v2 = Iterables.<JanusGraphVertex>getOnlyElement(v12.query().direction(Direction.OUT).labels("connect").vertices());
        assertEquals(v13, getOnlyElement(v2.query().direction(Direction.OUT).labels("link").vertices()));

        assertEquals(BaseVertexLabel.DEFAULT_VERTEXLABEL.name(), v.label());
        assertEquals("person", v12.label());
        assertEquals("person", v13.label());

        assertCount(4, tx.query().vertices());

        // ######### END INSPECTION & FAILURE ############

        clopen();

        // ######### INSPECTION & FAILURE (copied from above) ############
        assertEquals(v, getOnlyVertex(tx.query().has("uid", Cmp.EQUAL, "v1")));
        v = getOnlyVertex(tx.query().has("uid", Cmp.EQUAL, "v1"));
        v12 = getOnlyVertex(tx.query().has("uid", Cmp.EQUAL, "v12"));
        v13 = getOnlyVertex(tx.query().has("uid", Cmp.EQUAL, "v13"));
        try {
            //Invalid data type
            v.property("weight", "x");
            fail();
        } catch (SchemaViolationException e) {
        }
        try {
            //Only one "John" should be allowed
            v.property(VertexProperty.Cardinality.list, "name", "John");
            fail();
        } catch (SchemaViolationException e) {
        }
        try {
            //Cannot set a property as edge
            v.property("link", v);
            fail();
        } catch (IllegalArgumentException e) {
        }

        //Only one property for weight allowed
        v.property(VertexProperty.Cardinality.single, "weight", 1.0);
        assertCount(1, v.properties("weight"));
        v.property(VertexProperty.Cardinality.single, "weight", 0.5);
        assertEquals(0.5, v.<Float>value("weight").doubleValue(), 0.00001);
        assertEquals("v1", v.value("uid"));
        assertCount(2, v.properties("name"));
        for (Object o : v.query().labels("name").properties()) {
            JanusGraphVertexProperty<String> prop = (JanusGraphVertexProperty<String>) o;
            String nstr = prop.value();
            assertTrue(nstr.equals("Bob") || nstr.equals("John"));
        }
        assertTrue(Iterables.size(v.query().labels("value").properties()) >= 3);
        for (Object o : v.query().labels("value").properties()) {
            JanusGraphVertexProperty<Double> prop = (JanusGraphVertexProperty<Double>) o;
            double prec = prop.value().doubleValue();
            assertEquals(prec * 2, prop.<Number>value("weight").doubleValue(), 0.00001);
        }
        //Ensure we can add additional values
        p = v.property("value", 44.4, "weight", 88.8);
        assertEquals(v, getOnlyElement(tx.query().has("someid", Cmp.EQUAL, "Hello").vertices()));

        //------- EDGES -------
        try {
            //multiplicity violation
            v12.addEdge("parent", v13);
            fail();
        } catch (SchemaViolationException e) {
        }
        try {
            //multiplicity violation
            v13.addEdge("child", v12);
            fail();
        } catch (SchemaViolationException e) {
        }
        try {
            //multiplicity violation
            v13.addEdge("spouse", v12);
            fail();
        } catch (SchemaViolationException e) {
        }
        try {
            //multiplicity violation
            v.addEdge("spouse", v13);
            fail();
        } catch (SchemaViolationException e) {
        }
        assertCount(2, v.query().direction(Direction.IN).labels("parent").edges());
        assertCount(1, v12.query().direction(Direction.OUT).labels("parent").has("weight").edges());
        assertCount(1, v13.query().direction(Direction.OUT).labels("parent").has("weight").edges());
        assertEquals(v12, getOnlyElement(v.query().direction(Direction.OUT).labels("spouse").vertices()));
        edge = Iterables.<JanusGraphEdge>getOnlyElement(v.query().direction(Direction.BOTH).labels("connect").edges());
        assertEquals(1, edge.keys().size());
        assertEquals("e1", edge.value("uid"));
        try {
            //connect is simple
            v.addEdge("connect", v12);
            fail();
        } catch (SchemaViolationException e) {
        }
        //Make sure "link" is unidirected
        assertCount(1, v.query().direction(Direction.BOTH).labels("link").edges());
        assertCount(0, v13.query().direction(Direction.BOTH).labels("link").edges());
        //Assert we can add more friendships
        v.addEdge("friend", v12);
        v2 = Iterables.<JanusGraphVertex>getOnlyElement(v12.query().direction(Direction.OUT).labels("connect").vertices());
        assertEquals(v13, getOnlyElement(v2.query().direction(Direction.OUT).labels("link").vertices()));

        assertEquals(BaseVertexLabel.DEFAULT_VERTEXLABEL.name(), v.label());
        assertEquals("person", v12.label());
        assertEquals("person", v13.label());

        assertCount(4, tx.query().vertices());

        // ######### END INSPECTION & FAILURE ############

        //Ensure index uniqueness enforcement
        tx2 = graph.newTransaction();
        try {
            JanusGraphVertex vx = tx2.addVertex();
            try {
                //property is unique
                vx.property(VertexProperty.Cardinality.single, "uid", "v1");
                fail();
            } catch (SchemaViolationException e) {
            }
            vx.property(VertexProperty.Cardinality.single, "uid", "unique");
            JanusGraphVertex vx2 = tx2.addVertex();
            try {
                //property unique
                vx2.property(VertexProperty.Cardinality.single, "uid", "unique");
                fail();
            } catch (SchemaViolationException e) {
            }
        } finally {
            tx2.rollback();
        }


        //Ensure that v2 is really static
        v2 = getV(tx, v2);
        assertEquals("tweet", v2.label());
        try {
            v2.property(VertexProperty.Cardinality.single, "weight", 11);
            fail();
        } catch (SchemaViolationException e) {
        }
        try {
            v2.addEdge("friend", v12);
            fail();
        } catch (SchemaViolationException e) {
        }

        //Ensure that unidirected edges keep pointing to deleted vertices
        getV(tx, v13).remove();
        assertCount(1, v.query().direction(Direction.BOTH).labels("link").edges());

        //Finally, test the schema container
        SchemaContainer schemaContainer = new SchemaContainer(graph);
        assertTrue(schemaContainer.containsRelationType("weight"));
        assertTrue(schemaContainer.containsRelationType("friend"));
        assertTrue(schemaContainer.containsVertexLabel("person"));
        VertexLabelDefinition vld = schemaContainer.getVertexLabel("tag");
        assertFalse(vld.isPartitioned());
        assertFalse(vld.isStatic());
        PropertyKeyDefinition pkd = schemaContainer.getPropertyKey("name");
        assertEquals(Cardinality.SET, pkd.getCardinality());
        assertEquals(String.class, pkd.getDataType());
        EdgeLabelDefinition eld = schemaContainer.getEdgeLabel("child");
        assertEquals("child", eld.getName());
        assertEquals(child.longId(), eld.getLongId());
        assertEquals(Multiplicity.ONE2MANY, eld.getMultiplicity());
        assertFalse(eld.isUnidirected());
    }

    /**
     * Test the different data types that JanusGraph supports natively and ensure that invalid data types aren't allowed
     */
    @Test
    public void testDataTypes() throws Exception {
        clopen(option(CUSTOM_ATTRIBUTE_CLASS, "attribute10"), SpecialInt.class.getCanonicalName(),
                option(CUSTOM_SERIALIZER_CLASS, "attribute10"), SpecialIntSerializer.class.getCanonicalName());

        PropertyKey num = makeKey("num", SpecialInt.class);

        PropertyKey barr = makeKey("barr", byte[].class);

        PropertyKey boolval = makeKey("boolval", Boolean.class);

        PropertyKey birthday = makeKey("birthday", Instant.class);

        PropertyKey location = makeKey("location", Geoshape.class);

        PropertyKey boundary = makeKey("boundary", Geoshape.class);

        PropertyKey precise = makeKey("precise", Double.class);

        PropertyKey any = mgmt.makePropertyKey("any").cardinality(Cardinality.LIST).dataType(Object.class).make();

        try {
            //Not a valid data type - primitive
            makeKey("pint", int.class);
            fail();
        } catch (IllegalArgumentException e) {
        }

        try {
            //Not a valid data type - interface
            makeKey("number", Number.class);
            fail();
        } catch (IllegalArgumentException e) {
        }

        finishSchema();
        clopen();

        boolval = tx.getPropertyKey("boolval");
        num = tx.getPropertyKey("num");
        barr = tx.getPropertyKey("barr");
        birthday = tx.getPropertyKey("birthday");
        location = tx.getPropertyKey("location");
        boundary = tx.getPropertyKey("boundary");
        precise = tx.getPropertyKey("precise");
        any = tx.getPropertyKey("any");

        assertEquals(Boolean.class, boolval.dataType());
        assertEquals(byte[].class, barr.dataType());
        assertEquals(Object.class, any.dataType());

        final Instant c = Instant.ofEpochSecond(1429225756);
        final Geoshape point = Geoshape.point(10.0, 10.0);
        final Geoshape shape = Geoshape.box(10.0, 10.0, 20.0, 20.0);

        JanusGraphVertex v = tx.addVertex();
        v.property(n(boolval), true);
        v.property(VertexProperty.Cardinality.single, n(birthday), c);
        v.property(VertexProperty.Cardinality.single, n(num), new SpecialInt(10));
        v.property(VertexProperty.Cardinality.single, n(barr), new byte[]{1, 2, 3, 4});
        v.property(VertexProperty.Cardinality.single, n(location), point);
        v.property(VertexProperty.Cardinality.single, n(boundary), shape);
        v.property(VertexProperty.Cardinality.single, n(precise), 10.12345);
        v.property(n(any), "Hello");
        v.property(n(any), 10l);
        int[] testarr = {5, 6, 7};
        v.property(n(any), testarr);

        // ######## VERIFICATION ##########
        assertTrue(v.<Boolean>value("boolval"));
        assertEquals(10, v.<SpecialInt>value("num").getValue());
        assertEquals(c, v.value("birthday"));
        assertEquals(4, v.<byte[]>value("barr").length);
        assertEquals(point, v.<Geoshape>value("location"));
        assertEquals(shape, v.<Geoshape>value("boundary"));
        assertEquals(10.12345, v.<Double>value("precise").doubleValue(), 0.000001);
        assertCount(3, v.properties("any"));
        for (Object prop : v.query().labels("any").properties()) {
            Object value = ((JanusGraphVertexProperty<?>) prop).value();
            if (value instanceof String) assertEquals("Hello", value);
            else if (value instanceof Long) assertEquals(10l, value);
            else if (value.getClass().isArray()) {
                assertTrue(Arrays.equals(testarr, (int[]) value));
            } else fail();
        }

        clopen();

        v = getV(tx, v);

        // ######## VERIFICATION (copied from above) ##########
        assertTrue(v.<Boolean>value("boolval"));
        assertEquals(10, v.<SpecialInt>value("num").getValue());
        assertEquals(c, v.value("birthday"));
        assertEquals(4, v.<byte[]>value("barr").length);
        assertEquals(point, v.<Geoshape>value("location"));
        assertEquals(shape, v.<Geoshape>value("boundary"));
        assertEquals(10.12345, v.<Double>value("precise").doubleValue(), 0.000001);
        assertCount(3, v.properties("any"));
        for (Object prop : v.query().labels("any").properties()) {
            Object value = ((JanusGraphVertexProperty<?>) prop).value();
            if (value instanceof String) assertEquals("Hello", value);
            else if (value instanceof Long) assertEquals(10l, value);
            else if (value.getClass().isArray()) {
                assertTrue(Arrays.equals(testarr, (int[]) value));
            } else fail();
        }
    }

    /**
     * This tests a special scenario under which a schema type is defined in a (management) transaction
     * and then accessed in a concurrent transaction.
     * Also ensures that unique property values are enforced within and across transactions
     */
    @Test
    public void testTransactionalScopeOfSchemaTypes() {
        makeVertexIndexedUniqueKey("domain", String.class);
        finishSchema();

        Vertex v1, v2;
        v1 = tx.addVertex();
        try {
            v1.property(VertexProperty.Cardinality.single, "domain", "unique1");
        } catch (SchemaViolationException e) {

        } finally {
            tx.rollback();
            tx = null;
        }
        newTx();


        v1 = tx.addVertex();
        v1.property("domain", "unique1");
        try {
            v2 = tx.addVertex();
            v2.property("domain", "unique1");
            fail();
        } catch (SchemaViolationException e) {

        } finally {
            tx.rollback();
            tx = null;
        }
        newTx();

        clopen();
        v1 = tx.addVertex();
        v1.property("domain", "unique1");
        assertCount(1, tx.query().has("domain", "unique1").vertices());
        try {
            v2 = tx.addVertex();
            v2.property("domain", "unique1");
            fail();
        } catch (SchemaViolationException e) {

        } finally {
            tx.rollback();
            tx = null;
        }
        newTx();
    }

    /**
     * Tests the automatic creation of types
     */
    @Test
    public void testAutomaticTypeCreation() {
        assertFalse(tx.containsVertexLabel("person"));
        assertFalse(tx.containsVertexLabel("person"));
        assertFalse(tx.containsRelationType("value"));
        assertNull(tx.getPropertyKey("value"));
        PropertyKey value = tx.getOrCreatePropertyKey("value");
        assertNotNull(value);
        assertTrue(tx.containsRelationType("value"));
        JanusGraphVertex v = tx.addVertex("person");
        assertTrue(tx.containsVertexLabel("person"));
        assertEquals("person", v.label());
        assertFalse(tx.containsRelationType("knows"));
        Edge e = v.addEdge("knows", v);
        assertTrue(tx.containsRelationType("knows"));
        assertNotNull(tx.getEdgeLabel(e.label()));

        clopen(option(AUTO_TYPE), "none");

        assertTrue(tx.containsRelationType("value"));
        assertTrue(tx.containsVertexLabel("person"));
        assertTrue(tx.containsRelationType("knows"));
        v = getV(tx, v);

        //Cannot create new labels
        try {
            tx.addVertex("org");
            fail();
        } catch (IllegalArgumentException ex) {
        }
        try {
            v.property("bla", 5);
            fail();
        } catch (IllegalArgumentException ex) {
        }
        try {
            v.addEdge("blub", v);
            fail();
        } catch (IllegalArgumentException ex) {
        }
    }

    @Test
    public void testSchemaNameChange() {
        PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        EdgeLabel knows = mgmt.makeEdgeLabel("knows").multiplicity(Multiplicity.MULTI).make();
        mgmt.buildEdgeIndex(knows, "byTime", Direction.BOTH, time);
        mgmt.buildIndex("timeIndex", Vertex.class).addKey(time).buildCompositeIndex();
        mgmt.makeVertexLabel("people").make();
        finishSchema();

        //CREATE SMALL GRAPH
        JanusGraphVertex v = tx.addVertex("people");
        v.property(VertexProperty.Cardinality.single, "time", 5);
        v.addEdge("knows", v, "time", 11);

        newTx();
        v = Iterables.<JanusGraphVertex>getOnlyElement(tx.query().has("time", 5).vertices());
        assertNotNull(v);
        assertEquals("people", v.label());
        assertEquals(5, v.<Integer>value("time").intValue());
        assertCount(1, v.query().direction(Direction.IN).labels("knows").edges());
        assertCount(1, v.query().direction(Direction.OUT).labels("knows").has("time", 11).edges());
        newTx();

        //UPDATE SCHEMA NAMES

        assertTrue(mgmt.containsRelationType("knows"));
        knows = mgmt.getEdgeLabel("knows");
        mgmt.changeName(knows, "know");
        assertEquals("know", knows.name());

        assertTrue(mgmt.containsRelationIndex(knows, "byTime"));
        RelationTypeIndex rindex = mgmt.getRelationIndex(knows, "byTime");
        assertEquals("byTime", rindex.name());
        mgmt.changeName(rindex, "overTime");
        assertEquals("overTime", rindex.name());

        assertTrue(mgmt.containsVertexLabel("people"));
        VertexLabel vl = mgmt.getVertexLabel("people");
        mgmt.changeName(vl, "person");
        assertEquals("person", vl.name());

        assertTrue(mgmt.containsGraphIndex("timeIndex"));
        JanusGraphIndex gindex = mgmt.getGraphIndex("timeIndex");
        mgmt.changeName(gindex, "byTime");
        assertEquals("byTime", gindex.name());

        finishSchema();

        //VERIFY UPDATES IN MGMT SYSTEM

        assertTrue(mgmt.containsRelationType("know"));
        assertFalse(mgmt.containsRelationType("knows"));
        knows = mgmt.getEdgeLabel("know");

        assertTrue(mgmt.containsRelationIndex(knows, "overTime"));
        assertFalse(mgmt.containsRelationIndex(knows, "byTime"));

        assertTrue(mgmt.containsVertexLabel("person"));
        assertFalse(mgmt.containsVertexLabel("people"));

        assertTrue(mgmt.containsGraphIndex("byTime"));
        assertFalse(mgmt.containsGraphIndex("timeIndex"));

        //VERIFY UPDATES IN TRANSACTION
        newTx();
        v = Iterables.<JanusGraphVertex>getOnlyElement(tx.query().has("time", 5).vertices());
        assertNotNull(v);
        assertEquals("person", v.label());
        assertEquals(5, v.<Integer>value("time").intValue());
        assertCount(1, v.query().direction(Direction.IN).labels("know").edges());
        assertCount(0, v.query().direction(Direction.IN).labels("knows").edges());
        assertCount(1, v.query().direction(Direction.OUT).labels("know").has("time", 11).edges());
    }

    @Test
    public void testGotGLoadWithoutIndexBackendException() {
        try {
            GraphOfTheGodsFactory.load(graph);
            fail("Expected an exception to be thrown indicating improper index backend configuration");
        } catch (IllegalStateException ex) {
            assertTrue("An exception asking the user to use loadWithoutMixedIndex was expected",
                    ex.getMessage().contains("loadWithoutMixedIndex"));
        }
    }

    @Test
    public void testGotGIndexRemoval() throws InterruptedException, ExecutionException {
        clopen(option(LOG_SEND_DELAY, MANAGEMENT_LOG), Duration.ZERO,
                option(KCVSLog.LOG_READ_LAG_TIME, MANAGEMENT_LOG), Duration.ofMillis(50),
                option(LOG_READ_INTERVAL, MANAGEMENT_LOG), Duration.ofMillis(250)
        );

        final String name = "name";

        // Load Graph of the Gods
        GraphOfTheGodsFactory.loadWithoutMixedIndex(graph,
                true); // True makes the index on names unique.  Test fails when this is true.
        // Change to false and test will pass.
        newTx();
        finishSchema();

        JanusGraphIndex gindex = mgmt.getGraphIndex(name);

        // Sanity checks on the index that we assume GraphOfTheGodsFactory created
        assertNotNull(gindex);
        assertEquals(1, gindex.getFieldKeys().length);
        assertEquals(name, gindex.getFieldKeys()[0].name());
        assertEquals("internalindex", gindex.getBackingIndex());
        assertEquals(SchemaStatus.ENABLED, gindex.getIndexStatus(gindex.getFieldKeys()[0]));
        finishSchema();

        // Disable name index
        gindex = mgmt.getGraphIndex(name);
        mgmt.updateIndex(gindex, SchemaAction.DISABLE_INDEX);
        mgmt.commit();
        tx.commit();

        ManagementUtil.awaitGraphIndexUpdate(graph, name, 5, ChronoUnit.SECONDS);
        finishSchema();

        // Remove name index
        gindex = mgmt.getGraphIndex(name);
        mgmt.updateIndex(gindex, SchemaAction.REMOVE_INDEX);
        JanusGraphManagement.IndexJobFuture gmetrics = mgmt.getIndexJobStatus(gindex);
        finishSchema();

        // Should have deleted at least one record
        assertNotEquals(0, gmetrics.get().getCustom(IndexRemoveJob.DELETED_RECORDS_COUNT));
    }

    @Test
    public void testVertexCentricEdgeIndexOnSimpleMultiplicityShouldWork() {
        clopen(option(LOG_SEND_DELAY, MANAGEMENT_LOG), Duration.ofMillis(0),
                option(KCVSLog.LOG_READ_LAG_TIME, MANAGEMENT_LOG), Duration.ofMillis(50),
                option(LOG_READ_INTERVAL, MANAGEMENT_LOG), Duration.ofMillis(250)
        );
        PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        EdgeLabel friend = mgmt.makeEdgeLabel("friend").multiplicity(Multiplicity.SIMPLE).make();
        mgmt.buildEdgeIndex(friend, "byTime", Direction.OUT, decr, time);
        finishSchema();
        assertEquals(SchemaStatus.ENABLED, mgmt.getRelationIndex(mgmt.getRelationType("friend"), "byTime").getIndexStatus());
        JanusGraphVertex v = tx.addVertex();
        v = getV(tx, v);
        for (int i = 200; i < 210; i++) {
            JanusGraphVertex o = tx.addVertex();
            v.addEdge("friend", o, "time", i);
        }
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 199, 210).orderBy("time", decr),
            EDGE, 10, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        tx.commit();
        finishSchema();
    }

    @Test
    public void testVertexCentricPropertyIndexOnSetCardinalityShouldWork() {
        clopen(option(LOG_SEND_DELAY, MANAGEMENT_LOG), Duration.ofMillis(0),
                option(KCVSLog.LOG_READ_LAG_TIME, MANAGEMENT_LOG), Duration.ofMillis(50),
                option(LOG_READ_INTERVAL, MANAGEMENT_LOG), Duration.ofMillis(250)
        );
        PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SET).make();
        mgmt.buildPropertyIndex(name, "byTime", decr, time);
        finishSchema();
        assertEquals(SchemaStatus.ENABLED, mgmt.getRelationIndex(mgmt.getRelationType("name"), "byTime").getIndexStatus());
        JanusGraphVertex v = tx.addVertex();
        v = getV(tx, v);
        for (int i = 200; i < 210; i++) {
            v.property("name", String.valueOf(i), "time", i);
        }
        evaluateQuery(v.query().keys("name").interval("time", 199, 210).orderBy("time", decr),
            PROPERTY, 10, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        tx.commit();
        finishSchema();
    }

    @Test
    public void testVertexCentricIndexOrderingOnEdgePropertyWithCardinalityList() throws InterruptedException, ExecutionException {
        clopen(option(LOG_SEND_DELAY, MANAGEMENT_LOG), Duration.ofMillis(0),
                option(KCVSLog.LOG_READ_LAG_TIME, MANAGEMENT_LOG), Duration.ofMillis(50),
                option(LOG_READ_INTERVAL, MANAGEMENT_LOG), Duration.ofMillis(250)
        );
        PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).cardinality(Cardinality.LIST).make();
        EdgeLabel friend = mgmt.makeEdgeLabel("friend").multiplicity(Multiplicity.MULTI).make();
        mgmt.buildEdgeIndex(friend, "byTime", Direction.OUT, decr, time);
        finishSchema();
        JanusGraphVertex v = tx.addVertex();
        for (int i = 200; i < 210; i++) {
            JanusGraphVertex o = tx.addVertex();
            v.addEdge("friend", o, "time", i);
        }
        assertEquals(SchemaStatus.ENABLED, mgmt.getRelationIndex(mgmt.getRelationType("friend"), "byTime").getIndexStatus());
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 201, 205).orderBy("time", decr),
            EDGE, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        tx.commit();
        finishSchema();
    }

    @Test
    public void testVertexCentricIndexOrderingOnMetaPropertyWithCardinalityList() throws InterruptedException, ExecutionException {
        clopen(option(LOG_SEND_DELAY, MANAGEMENT_LOG), Duration.ofMillis(0),
                option(KCVSLog.LOG_READ_LAG_TIME, MANAGEMENT_LOG), Duration.ofMillis(50),
                option(LOG_READ_INTERVAL, MANAGEMENT_LOG), Duration.ofMillis(250)
        );
        PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).cardinality(Cardinality.LIST).make();
        PropertyKey sensor = mgmt.makePropertyKey("sensor").dataType(Integer.class).cardinality(Cardinality.LIST).make();
        mgmt.buildPropertyIndex(sensor, "byTime", decr, time);
        finishSchema();
        JanusGraphVertex v = tx.addVertex();
        for (int i = 200; i < 210; i++) {
            v.property("sensor", i, "time", i);
        }
        assertEquals(SchemaStatus.ENABLED, mgmt.getRelationIndex(mgmt.getRelationType("sensor"), "byTime").getIndexStatus());
        evaluateQuery(v.query().keys("sensor").interval("time", 201, 205).orderBy("time", decr),
            PROPERTY, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        tx.commit();
        finishSchema();
    }

    @Test
    public void testIndexUpdatesWithReindexAndRemove() throws InterruptedException, ExecutionException {
        clopen(option(LOG_SEND_DELAY, MANAGEMENT_LOG), Duration.ofMillis(0),
                option(KCVSLog.LOG_READ_LAG_TIME, MANAGEMENT_LOG), Duration.ofMillis(50),
                option(LOG_READ_INTERVAL, MANAGEMENT_LOG), Duration.ofMillis(250)
        );
        //Types without index
        PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).make();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SET).make();
        EdgeLabel friend = mgmt.makeEdgeLabel("friend").multiplicity(Multiplicity.MULTI).make();
        PropertyKey sensor = mgmt.makePropertyKey("sensor").dataType(Double.class).cardinality(Cardinality.LIST).make();
        finishSchema();

        RelationTypeIndex pindex, eindex;
        JanusGraphIndex gindex;

        //Add some sensor & friend data
        JanusGraphVertex v = tx.addVertex();
        for (int i = 0; i < 10; i++) {
            v.property("sensor", i, "time", i);
            v.property("name", "v" + i);
            JanusGraphVertex o = tx.addVertex();
            v.addEdge("friend", o, "time", i);
        }
        newTx();
        //Indexes should not yet be in use
        v = getV(tx, v);
        evaluateQuery(v.query().keys("sensor").interval("time", 1, 5).orderBy("time", decr),
                PROPERTY, 4, 1, new boolean[]{false, false}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().keys("sensor").interval("time", 101, 105).orderBy("time", decr),
                PROPERTY, 0, 1, new boolean[]{false, false}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 1, 5).orderBy("time", decr),
                EDGE, 4, 1, new boolean[]{false, false}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 101, 105).orderBy("time", decr),
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
        mgmt.buildPropertyIndex(sensor, "byTime", decr, time);
        mgmt.buildEdgeIndex(friend, "byTime", Direction.OUT, decr, time);
        mgmt.buildIndex("bySensorReading", Vertex.class).addKey(name).buildCompositeIndex();
        finishSchema();
        newTx();
        //Add some sensor & friend data that should already be indexed even though index is not yet enabled
        v = getV(tx, v);
        for (int i = 100; i < 110; i++) {
            v.property("sensor", i, "time", i);
            v.property("name", "v" + i);
            JanusGraphVertex o = tx.addVertex();
            v.addEdge("friend", o, "time", i);
        }
        tx.commit();
        //Should not yet be able to enable since not yet registered
        pindex = mgmt.getRelationIndex(mgmt.getRelationType("sensor"), "byTime");
        eindex = mgmt.getRelationIndex(mgmt.getRelationType("friend"), "byTime");
        gindex = mgmt.getGraphIndex("bySensorReading");
        try {
            mgmt.updateIndex(pindex, SchemaAction.ENABLE_INDEX);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            mgmt.updateIndex(eindex, SchemaAction.ENABLE_INDEX);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            mgmt.updateIndex(gindex, SchemaAction.ENABLE_INDEX);
            fail();
        } catch (IllegalArgumentException e) {
        }
        mgmt.commit();


        ManagementUtil.awaitVertexIndexUpdate(graph, "byTime", "sensor", 10, ChronoUnit.SECONDS);
        ManagementUtil.awaitGraphIndexUpdate(graph, "bySensorReading", 5, ChronoUnit.SECONDS);

        finishSchema();
        //Verify new status
        pindex = mgmt.getRelationIndex(mgmt.getRelationType("sensor"), "byTime");
        eindex = mgmt.getRelationIndex(mgmt.getRelationType("friend"), "byTime");
        gindex = mgmt.getGraphIndex("bySensorReading");
        assertEquals(SchemaStatus.REGISTERED, pindex.getIndexStatus());
        assertEquals(SchemaStatus.REGISTERED, eindex.getIndexStatus());
        assertEquals(SchemaStatus.REGISTERED, gindex.getIndexStatus(gindex.getFieldKeys()[0]));
        finishSchema();
        //Simply enable without reindex
        eindex = mgmt.getRelationIndex(mgmt.getRelationType("friend"), "byTime");
        mgmt.updateIndex(eindex, SchemaAction.ENABLE_INDEX);
        finishSchema();
        assertTrue(ManagementSystem.awaitRelationIndexStatus(graph, "byTime", "friend").status(SchemaStatus.ENABLED)
                .timeout(10L, ChronoUnit.SECONDS).call().getSucceeded());

        //Reindex the other two
        pindex = mgmt.getRelationIndex(mgmt.getRelationType("sensor"), "byTime");
        ScanMetrics reindexSensorByTime = mgmt.updateIndex(pindex, SchemaAction.REINDEX).get();
        finishSchema();
        gindex = mgmt.getGraphIndex("bySensorReading");
        ScanMetrics reindexBySensorReading = mgmt.updateIndex(gindex, SchemaAction.REINDEX).get();
        finishSchema();

        assertNotEquals(0, reindexSensorByTime.getCustom(IndexRepairJob.ADDED_RECORDS_COUNT));
        assertNotEquals(0, reindexBySensorReading.getCustom(IndexRepairJob.ADDED_RECORDS_COUNT));

        //Every index should now be enabled
        pindex = mgmt.getRelationIndex(mgmt.getRelationType("sensor"), "byTime");
        eindex = mgmt.getRelationIndex(mgmt.getRelationType("friend"), "byTime");
        gindex = mgmt.getGraphIndex("bySensorReading");
        assertEquals(SchemaStatus.ENABLED, eindex.getIndexStatus());
        assertEquals(SchemaStatus.ENABLED, pindex.getIndexStatus());
        assertEquals(SchemaStatus.ENABLED, gindex.getIndexStatus(gindex.getFieldKeys()[0]));


        //Add some more sensor & friend data
        newTx();
        v = getV(tx, v);
        for (int i = 200; i < 210; i++) {
            v.property("sensor", i, "time", i);
            v.property("name", "v" + i);
            JanusGraphVertex o = tx.addVertex();
            v.addEdge("friend", o, "time", i);
        }
        newTx();
        //Use indexes now but only see new data for property and graph index
        v = getV(tx, v);
        evaluateQuery(v.query().keys("sensor").interval("time", 1, 5).orderBy("time", decr),
                PROPERTY, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().keys("sensor").interval("time", 101, 105).orderBy("time", decr),
                PROPERTY, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().keys("sensor").interval("time", 201, 205).orderBy("time", decr),
                PROPERTY, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 1, 5).orderBy("time", decr),
                EDGE, 0, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 101, 105).orderBy("time", decr),
                EDGE, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 201, 205).orderBy("time", decr),
                EDGE, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(tx.query().has("name", "v5"),
                ElementCategory.VERTEX, 1, new boolean[]{true, true}, "bySensorReading");
        evaluateQuery(tx.query().has("name", "v105"),
                ElementCategory.VERTEX, 1, new boolean[]{true, true}, "bySensorReading");
        evaluateQuery(tx.query().has("name", "v205"),
                ElementCategory.VERTEX, 1, new boolean[]{true, true}, "bySensorReading");

        finishSchema();
        eindex = mgmt.getRelationIndex(mgmt.getRelationType("friend"), "byTime");
        ScanMetrics reindexFriendByTime = mgmt.updateIndex(eindex, SchemaAction.REINDEX).get();
        finishSchema();
        assertNotEquals(0, reindexFriendByTime.getCustom(IndexRepairJob.ADDED_RECORDS_COUNT));

        finishSchema();
        newTx();
        //It should now have all the answers
        v = getV(tx, v);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 1, 5).orderBy("time", decr),
                EDGE, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 101, 105).orderBy("time", decr),
                EDGE, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 201, 205).orderBy("time", decr),
                EDGE, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);

        pindex = mgmt.getRelationIndex(mgmt.getRelationType("sensor"), "byTime");
        gindex = mgmt.getGraphIndex("bySensorReading");
        mgmt.updateIndex(pindex, SchemaAction.DISABLE_INDEX);
        mgmt.updateIndex(gindex, SchemaAction.DISABLE_INDEX);
        mgmt.commit();
        tx.commit();

        ManagementUtil.awaitVertexIndexUpdate(graph, "byTime", "sensor", 10, ChronoUnit.SECONDS);
        ManagementUtil.awaitGraphIndexUpdate(graph, "bySensorReading", 5, ChronoUnit.SECONDS);

        finishSchema();

        pindex = mgmt.getRelationIndex(mgmt.getRelationType("sensor"), "byTime");
        gindex = mgmt.getGraphIndex("bySensorReading");
        assertEquals(SchemaStatus.DISABLED, pindex.getIndexStatus());
        assertEquals(SchemaStatus.DISABLED, gindex.getIndexStatus(gindex.getFieldKeys()[0]));
        finishSchema();

        newTx();
        //The two disabled indexes should force full scans
        v = getV(tx, v);
        evaluateQuery(v.query().keys("sensor").interval("time", 1, 5).orderBy("time", decr),
                PROPERTY, 4, 1, new boolean[]{false, false}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().keys("sensor").interval("time", 101, 105).orderBy("time", decr),
                PROPERTY, 4, 1, new boolean[]{false, false}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().keys("sensor").interval("time", 201, 205).orderBy("time", decr),
                PROPERTY, 4, 1, new boolean[]{false, false}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 1, 5).orderBy("time", decr),
                EDGE, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 101, 105).orderBy("time", decr),
                EDGE, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 201, 205).orderBy("time", decr),
                EDGE, 4, 1, new boolean[]{true, true}, tx.getPropertyKey("time"), Order.DESC);
        evaluateQuery(tx.query().has("name", "v5"),
                ElementCategory.VERTEX, 1, new boolean[]{false, true});
        evaluateQuery(tx.query().has("name", "v105"),
                ElementCategory.VERTEX, 1, new boolean[]{false, true});
        evaluateQuery(tx.query().has("name", "v205"),
                ElementCategory.VERTEX, 1, new boolean[]{false, true});

        tx.commit();
        finishSchema();
        pindex = mgmt.getRelationIndex(mgmt.getRelationType("sensor"), "byTime");
        gindex = mgmt.getGraphIndex("bySensorReading");
        ScanMetrics pmetrics = mgmt.updateIndex(pindex, SchemaAction.REMOVE_INDEX).get();
        ScanMetrics gmetrics = mgmt.updateIndex(gindex, SchemaAction.REMOVE_INDEX).get();
        finishSchema();
        assertEquals(30, pmetrics.getCustom(IndexRemoveJob.DELETED_RECORDS_COUNT));
        assertEquals(30, gmetrics.getCustom(IndexRemoveJob.DELETED_RECORDS_COUNT));
    }

    @Category({BrittleTests.class})
    @Test
    public void testIndexUpdateSyncWithMultipleInstances() throws InterruptedException {
        clopen(option(LOG_SEND_DELAY, MANAGEMENT_LOG), Duration.ofMillis(0),
                option(KCVSLog.LOG_READ_LAG_TIME, MANAGEMENT_LOG), Duration.ofMillis(50),
                option(LOG_READ_INTERVAL, MANAGEMENT_LOG), Duration.ofMillis(250)
        );

        StandardJanusGraph graph2 = (StandardJanusGraph) JanusGraphFactory.open(config);
        JanusGraphTransaction tx2;

        mgmt.makePropertyKey("name").dataType(String.class).make();
        finishSchema();

        tx.addVertex("name", "v1");
        newTx();
        evaluateQuery(tx.query().has("name", "v1"), ElementCategory.VERTEX, 1, new boolean[]{false, true});
        tx2 = graph2.newTransaction();
        evaluateQuery(tx2.query().has("name", "v1"), ElementCategory.VERTEX, 1, new boolean[]{false, true});
        //Leave tx2 open to delay acknowledgement

        mgmt.buildIndex("theIndex", Vertex.class).addKey(mgmt.getPropertyKey("name")).buildCompositeIndex();
        mgmt.commit();

        JanusGraphTransaction tx3 = graph2.newTransaction();
        tx3.addVertex("name", "v2");
        tx3.commit();
        newTx();
        tx.addVertex("name", "v3");
        tx.commit();

        finishSchema();
        try {
            mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.ENABLE_INDEX);
            fail(); //Open tx2 should not make this possible
        } catch (IllegalArgumentException e) {
        }
        finishSchema();
        tx2.commit(); //Release transaction and wait a little for registration which should make enabling possible
        mgmt.rollback();
        assertTrue(ManagementSystem.awaitGraphIndexStatus(graph, "theIndex").status(SchemaStatus.REGISTERED)
                .timeout(TestGraphConfigs.getSchemaConvergenceTime(ChronoUnit.SECONDS), ChronoUnit.SECONDS)
                .call().getSucceeded());
        finishSchema();
        mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.ENABLE_INDEX);
        finishSchema();

        tx2 = graph2.newTransaction();
        tx2.addVertex("name", "v4"); //Should be added to index but index not yet enabled
        tx2.commit();

        newTx();
        evaluateQuery(tx.query().has("name", "v1"), ElementCategory.VERTEX, 0, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().has("name", "v2"), ElementCategory.VERTEX, 1, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().has("name", "v3"), ElementCategory.VERTEX, 1, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().has("name", "v4"), ElementCategory.VERTEX, 1, new boolean[]{true, true}, "theIndex");

        tx2 = graph2.newTransaction();
        evaluateQuery(tx2.query().has("name", "v1"), ElementCategory.VERTEX, 0, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx2.query().has("name", "v2"), ElementCategory.VERTEX, 1, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx2.query().has("name", "v3"), ElementCategory.VERTEX, 1, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx2.query().has("name", "v4"), ElementCategory.VERTEX, 1, new boolean[]{true, true}, "theIndex");
        tx2.commit();

        //Finally test retrieving and closing open instances

        Set<String> openInstances = mgmt.getOpenInstances();
        assertEquals(2, openInstances.size());
        assertTrue(openInstances.contains(graph.getConfiguration().getUniqueGraphId() + "(current)"));
        assertTrue(openInstances.contains(graph2.getConfiguration().getUniqueGraphId()));
        try {
            mgmt.forceCloseInstance(graph.getConfiguration().getUniqueGraphId());
            fail(); //Cannot close current instance
        } catch (IllegalArgumentException e) {
        }
        mgmt.forceCloseInstance(graph2.getConfiguration().getUniqueGraphId());

        graph2.close();

    }

    @Category({BrittleTests.class})
    @Test
    public void testIndexShouldRegisterWhenWeRemoveAnInstance() throws InterruptedException {
        clopen(option(LOG_SEND_DELAY, MANAGEMENT_LOG), Duration.ofMillis(0),
                option(KCVSLog.LOG_READ_LAG_TIME, MANAGEMENT_LOG), Duration.ofMillis(50),
                option(LOG_READ_INTERVAL, MANAGEMENT_LOG), Duration.ofMillis(250)
        );

        StandardJanusGraph graph2 = (StandardJanusGraph) JanusGraphFactory.open(config);
        JanusGraphTransaction tx2;

        mgmt.makePropertyKey("name").dataType(String.class).make();
        finishSchema();

        tx.addVertex("name", "v1");
        newTx();
        evaluateQuery(tx.query().has("name", "v1"), ElementCategory.VERTEX, 1, new boolean[]{false, true});
        tx2 = graph2.newTransaction();
        evaluateQuery(tx2.query().has("name", "v1"), ElementCategory.VERTEX, 1, new boolean[]{false, true});
        //Leave tx2 open to delay acknowledgement

        mgmt.buildIndex("theIndex", Vertex.class).addKey(mgmt.getPropertyKey("name")).buildCompositeIndex();
        mgmt.commit();

        JanusGraphTransaction tx3 = graph2.newTransaction();
        tx3.addVertex("name", "v2");
        tx3.commit();
        newTx();
        tx.addVertex("name", "v3");
        tx.commit();

        finishSchema();
        try {
            mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.ENABLE_INDEX);
            fail(); //Open tx2 should not make this possible
        } catch (IllegalArgumentException e) {
        }
        finishSchema();

        //close second graph instance, so index can move to REGISTERED
        Set<String> openInstances = mgmt.getOpenInstances();
        assertEquals(2, openInstances.size());
        assertTrue(openInstances.contains(graph.getConfiguration().getUniqueGraphId() + "(current)"));
        assertTrue(openInstances.contains(graph2.getConfiguration().getUniqueGraphId()));
        try {
            mgmt.forceCloseInstance(graph.getConfiguration().getUniqueGraphId());
            fail(); //Cannot close current instance
        } catch (IllegalArgumentException e) {
        }
        mgmt.forceCloseInstance(graph2.getConfiguration().getUniqueGraphId());

        mgmt.commit();
        assertTrue(ManagementSystem.awaitGraphIndexStatus(graph, "theIndex").status(SchemaStatus.REGISTERED)
                .timeout(TestGraphConfigs.getSchemaConvergenceTime(ChronoUnit.SECONDS), ChronoUnit.SECONDS)
                .call().getSucceeded());
        finishSchema();
        mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.ENABLE_INDEX);
        finishSchema();
    }

    @Test
    public void testIndexShouldBeEnabledForExistingPropertyKeyAndConstrainedToNewVertexLabel() {
        mgmt.makePropertyKey("alreadyExistingProperty").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        finishSchema();

        PropertyKey existingPropertyKey  = mgmt.getPropertyKey("alreadyExistingProperty");
        VertexLabel newLabel = mgmt.makeVertexLabel("newLabel").make();
        mgmt.buildIndex("newIndex", Vertex.class).addKey(existingPropertyKey).indexOnly(newLabel).buildCompositeIndex();
        finishSchema();

        assertEquals(SchemaStatus.ENABLED, mgmt.getGraphIndex("newIndex").getIndexStatus(existingPropertyKey));
    }

    @Test
    public void testIndexShouldBeEnabledForExistingPropertyKeyAndConstrainedToNewEdgeLabel() {
        mgmt.makePropertyKey("alreadyExistingProperty").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        finishSchema();

        PropertyKey existingPropertyKey  = mgmt.getPropertyKey("alreadyExistingProperty");
        EdgeLabel newLabel = mgmt.makeEdgeLabel("newLabel").make();
        mgmt.buildIndex("newIndex", Edge.class).addKey(existingPropertyKey).indexOnly(newLabel).buildCompositeIndex();
        finishSchema();

        assertEquals(SchemaStatus.ENABLED, mgmt.getGraphIndex("newIndex").getIndexStatus(existingPropertyKey));
    }

    @Test
    public void testIndexShouldNotBeEnabledForExistingPropertyKeyAndConstrainedToExistingVertexLabel() {
        mgmt.makePropertyKey("alreadyExistingProperty").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makeVertexLabel("alreadyExistingLabel").make();
        finishSchema();

        PropertyKey existingPropertyKey  = mgmt.getPropertyKey("alreadyExistingProperty");
        VertexLabel existingLabel = mgmt.getVertexLabel("alreadyExistingLabel");
        mgmt.buildIndex("newIndex", Vertex.class).addKey(existingPropertyKey).indexOnly(existingLabel).buildCompositeIndex();
        finishSchema();

        assertNotEquals(SchemaStatus.ENABLED, mgmt.getGraphIndex("newIndex").getIndexStatus(existingPropertyKey));
    }

    @Test
    public void testIndexShouldNotBeEnabledForExistingPropertyKeyAndConstrainedToExistingEdgeLabel() {
        mgmt.makePropertyKey("alreadyExistingProperty").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makeEdgeLabel("alreadyExistingLabel").make();
        finishSchema();

        PropertyKey existingPropertyKey  = mgmt.getPropertyKey("alreadyExistingProperty");
        EdgeLabel existingLabel = mgmt.getEdgeLabel("alreadyExistingLabel");
        mgmt.buildIndex("newIndex", Edge.class).addKey(existingPropertyKey).indexOnly(existingLabel).buildCompositeIndex();
        finishSchema();

        assertNotEquals(SchemaStatus.ENABLED, mgmt.getGraphIndex("newIndex").getIndexStatus(existingPropertyKey));
    }

    @Test
    public void testIndexShouldNotBeEnabledForExistingPropertyKeyWithoutLabelConstraint() {
        mgmt.makePropertyKey("alreadyExistingProperty").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        finishSchema();

        PropertyKey existingPropertyKey  = mgmt.getPropertyKey("alreadyExistingProperty");
        mgmt.buildIndex("newIndex", Vertex.class).addKey(existingPropertyKey).buildCompositeIndex();
        finishSchema();

        assertNotEquals(SchemaStatus.ENABLED, mgmt.getGraphIndex("newIndex").getIndexStatus(existingPropertyKey));
    }

   /* ==================================================================================
                            ADVANCED
     ==================================================================================*/

    /**
     * This test exercises different types of updates against cardinality restricted properties
     * to ensure that the resulting behavior is fully consistent.
     */
    @Test
    public void testPropertyCardinality() {
        PropertyKey uid = mgmt.makePropertyKey("uid").dataType(Long.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex("byUid", Vertex.class).addKey(uid).unique().buildCompositeIndex();
        mgmt.buildIndex("byName", Vertex.class).addKey(name).buildCompositeIndex();

        finishSchema();

        JanusGraphVertex v1 = tx.addVertex();
        v1.property("name", "name1");
        JanusGraphVertex v2 = tx.addVertex();
        v2.property("uid", 512);

        newTx();

        v1 = tx.getVertex(v1.longId());
        v1.property("name", "name2"); //Ensure that the old index record gets removed
        v2 = tx.getVertex(v2.longId());
        v2.property("uid", 512); //Ensure that replacement is allowed

        newTx();

        assertCount(0, tx.query().has("name", "name1").vertices());
        assertCount(1, tx.query().has("name", "name2").vertices());
        assertCount(1, tx.query().has("uid", 512).vertices());
    }

    /**
     * Test the correct application of {@link org.janusgraph.graphdb.types.system.ImplicitKey}
     * to vertices, edges, and properties.
     * <p>
     * Additionally tests RelationIdentifier since this is closely related to ADJACENT and JANUSGRAPHID implicit keys.
     */
    @Test
    public void testImplicitKey() {
        JanusGraphVertex v = graph.addVertex("name", "Dan"), u = graph.addVertex();
        Edge e = v.addEdge("knows", u);
        graph.tx().commit();
        RelationIdentifier eid = (RelationIdentifier) e.id();

        assertEquals(v.id(), v.value(ID_NAME));
        assertEquals(eid, e.value(ID_NAME));
        assertEquals("knows", e.value(LABEL_NAME));
        assertEquals(BaseVertexLabel.DEFAULT_VERTEXLABEL.name(), v.value(LABEL_NAME));
        assertCount(1, v.query().direction(Direction.BOTH).labels("knows").has(ID_NAME, eid).edges());
        assertCount(0, v.query().direction(Direction.BOTH).labels("knows").has(ID_NAME, RelationIdentifier.get(new long[]{4, 5, 6, 7})).edges());
        assertCount(1, v.query().direction(Direction.BOTH).labels("knows").has("~nid", eid.getRelationId()).edges());
        assertCount(0, v.query().direction(Direction.BOTH).labels("knows").has("~nid", 110111).edges());
        //Test edge retrieval
        assertNotNull(getE(graph, eid));
        assertEquals(eid, getE(graph, eid).id());
        //Test adjacent constraint
        assertEquals(1, v.query().direction(BOTH).has("~adjacent", u.id()).edgeCount());
        assertCount(1, v.query().direction(BOTH).has("~adjacent", (int) getId(u)).edges());
        try {
            //Not a valid vertex
            assertCount(0, v.query().direction(BOTH).has("~adjacent", 110111).edges());
            fail();
        } catch (IllegalArgumentException ex) {
        }

    }

    @Test
    public void testArrayEqualityUsingImplicitKey() {
        JanusGraphVertex v = graph.addVertex();

        byte singleDimension[] = new byte[]{127, 0, 0, 1};
        byte singleDimensionCopy[] = new byte[]{127, 0, 0, 1};
        final String singlePropName = "single";

        v.property(singlePropName, singleDimension);

        assertEquals(1, Iterables.size(graph.query().has(singlePropName, singleDimension).vertices()));
        assertEquals(1, Iterables.size(graph.query().has(singlePropName, singleDimensionCopy).vertices()));

        graph.tx().commit();

        assertEquals(1, Iterables.size(graph.query().has(singlePropName, singleDimension).vertices()));
        assertEquals(1, Iterables.size(graph.query().has(singlePropName, singleDimensionCopy).vertices()));
    }

    /**
     * Tests that self-loop edges are handled and counted correctly
     */
    @Test
    public void testSelfLoop() {
        JanusGraphVertex v = tx.addVertex();
        v.addEdge("self", v);
        assertCount(1, v.query().direction(Direction.OUT).labels("self").edges());
        assertCount(1, v.query().direction(Direction.IN).labels("self").edges());
        assertCount(2, v.query().direction(Direction.BOTH).labels("self").edges());
        clopen();
        v = getV(tx, v);
        assertNotNull(v);
        assertCount(1, v.query().direction(Direction.IN).labels("self").edges());
        assertCount(1, v.query().direction(Direction.OUT).labels("self").edges());
        assertCount(1, v.query().direction(Direction.IN).labels("self").edges());
        assertCount(2, v.query().direction(Direction.BOTH).labels("self").edges());
    }

    /**
     * Tests that elements can be accessed beyond their transactional boundaries if they
     * are bound to single-threaded graph transactions
     */
    @Test
    @SuppressWarnings("deprecation")
    public void testThreadBoundTx() {
        PropertyKey t = mgmt.makePropertyKey("type").dataType(Integer.class).make();
        mgmt.buildIndex("etype", Edge.class).addKey(t).buildCompositeIndex();
        ((StandardEdgeLabelMaker) mgmt.makeEdgeLabel("friend")).sortKey(t).make();
        finishSchema();

        JanusGraphVertex v1 = graph.addVertex("name", "Vertex1", "age", 35);
        JanusGraphVertex v2 = graph.addVertex("name", "Vertex2", "age", 45);
        JanusGraphVertex v3 = graph.addVertex("name", "Vertex3", "age", 55);

        Edge e1 = v1.addEdge("knows", v2, "time", 5);
        Edge e2 = v2.addEdge("knows", v3, "time", 15);
        Edge e3 = v3.addEdge("knows", v1, "time", 25);
        Edge e4 = v2.addEdge("friend", v2, "type", 1);
        for (JanusGraphVertex v : new JanusGraphVertex[]{v1, v2, v3}) {
            assertCount(2, v.query().direction(Direction.BOTH).labels("knows").edges());
            assertCount(1, v.query().direction(Direction.OUT).labels("knows").edges());
            JanusGraphEdge tmpE = Iterables.<JanusGraphEdge>getOnlyElement(v.query().direction(Direction.OUT).labels("knows").edges());
            assertEquals(5, tmpE.<Integer>value("time") % 10);
        }
        e3.property("time", 35);
        assertEquals(35, e3.<Integer>value("time").intValue());

        v1.addEdge("friend", v2, "type", 0);
        graph.tx().commit();
        e4.property("type", 2);
        JanusGraphEdge ef = Iterables.<JanusGraphEdge>getOnlyElement(v1.query().direction(Direction.OUT).labels("friend").edges());
        assertEquals(ef, getOnlyElement(graph.query().has("type", 0).edges()));
        ef.property("type", 1);
        graph.tx().commit();

        assertEquals(35, e3.<Integer>value("time").intValue());
        e3 = getE(graph, e3);
        e3.property("time", 45);
        assertEquals(45, e3.<Integer>value("time").intValue());

        assertEquals(15, e2.<Integer>value("time").intValue());
        e2.property("time", 25);
        assertEquals(25, e2.<Integer>value("time").intValue());

        assertEquals(35, v1.<Integer>value("age").intValue());
        assertEquals(55, v3.<Integer>value("age").intValue());
        v3.property("age", 65);
        assertEquals(65, v3.<Integer>value("age").intValue());
        e1 = getE(graph, e1);

        for (JanusGraphVertex v : new JanusGraphVertex[]{v1, v2, v3}) {
            assertCount(2, v.query().direction(Direction.BOTH).labels("knows").edges());
            assertCount(1, v.query().direction(Direction.OUT).labels("knows").edges());
            assertEquals(5, ((Edge) getOnlyElement(v.query().direction(Direction.OUT).labels("knows").edges())).<Integer>value("time").intValue() % 10);
        }

        graph.tx().commit();

        VertexProperty prop = v1.properties().next();
        assertTrue(getId(prop) > 0);
        prop = (VertexProperty) ((Iterable) graph.multiQuery(v1).properties().values().iterator().next()).iterator().next();
        assertTrue(getId(prop) > 0);

        assertEquals(45, e3.<Integer>value("time").intValue());
        assertEquals(5, e1.<Integer>value("time").intValue());

        assertEquals(35, v1.<Integer>value("age").intValue());
        assertEquals(65, v3.<Integer>value("age").intValue());

        for (JanusGraphVertex v : new JanusGraphVertex[]{v1, v2, v3}) {
            assertCount(2, v.query().direction(Direction.BOTH).labels("knows").edges());
            assertCount(1, v.query().direction(Direction.OUT).labels("knows").edges());
            assertEquals(5, ((Edge) getOnlyElement(v.query().direction(Direction.OUT).labels("knows").edges())).<Integer>value("time").intValue() % 10);
        }

        graph.tx().commit();

        v1 = graph.addVertex();
        v2 = graph.addVertex();
        v1.addEdge("knows", v2);
        graph.tx().commit();
        v3 = graph.addVertex();
        Edge e = v1.addEdge("knows", v3);
        assertFalse(e.property("age").isPresent());
    }

    @Test
    public void testNestedTransactions() {
        Vertex v1 = graph.addVertex();
        newTx();
        Vertex v2 = tx.addVertex();
        v2.property("name", "foo");
        tx.commit();
        v1.addEdge("related", graph.traversal().V(v2).next());
        graph.tx().commit();
        assertCount(1, v1.edges(OUT));
    }

    @Test
    public void testStaleVertex() {
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.makePropertyKey("age").dataType(Integer.class).make();
        mgmt.buildIndex("byName", Vertex.class).addKey(name).unique().buildCompositeIndex();
        finishSchema();


        JanusGraphVertex cartman = graph.addVertex("name", "cartman", "age", 10);
        graph.addVertex("name", "stan", "age", 8);

        graph.tx().commit();

        cartman = Iterables.<JanusGraphVertex>getOnlyElement(graph.query().has("name", "cartman").vertices());

        graph.tx().commit();

        JanusGraphVertexProperty p = (JanusGraphVertexProperty) cartman.properties().next();
        assertTrue((p.longId()) > 0);
        graph.tx().commit();
    }

    /**
     * Verifies transactional isolation and internal vertex existence checking
     */
    @Test
    public void testTransactionIsolation() {
        // Create edge label before attempting to write it from concurrent transactions
        makeLabel("knows");
        finishSchema();

        JanusGraphTransaction tx1 = graph.newTransaction();
        JanusGraphTransaction tx2 = graph.newTransaction();

        //Verify that using vertices across transactions is prohibited
        JanusGraphVertex v11 = tx1.addVertex();
        JanusGraphVertex v12 = tx1.addVertex();
        v11.addEdge("knows", v12);

        JanusGraphVertex v21 = tx2.addVertex();
        try {
            v21.addEdge("knows", v11);
            fail();
        } catch (IllegalStateException e) {
        }
        JanusGraphVertex v22 = tx2.addVertex();
        v21.addEdge("knows", v22);
        tx2.commit();
        try {
            v22.addEdge("knows", v21);
            fail();
        } catch (IllegalStateException e) {
        }
        tx1.rollback();
        try {
            v11.property(VertexProperty.Cardinality.single, "test", 5);
            fail();
        } catch (IllegalStateException e) {
        }

        //Test unidirected edge with and without internal existence check
        newTx();
        v21 = getV(tx, v21);
        tx.makeEdgeLabel("link").unidirected().make();
        JanusGraphVertex v3 = tx.addVertex();
        v21.addEdge("link", v3);
        newTx();
        v21 = getV(tx, v21);
        v3 = Iterables.<JanusGraphVertex>getOnlyElement(v21.query().direction(Direction.OUT).labels("link").vertices());
        assertFalse(v3.isRemoved());
        v3.remove();
        newTx();
        v21 = getV(tx, v21);
        v3 = Iterables.<JanusGraphVertex>getOnlyElement(v21.query().direction(Direction.OUT).labels("link").vertices());
        assertFalse(v3.isRemoved());
        newTx();

        JanusGraphTransaction tx3 = graph.buildTransaction().checkInternalVertexExistence(true).start();
        v21 = getV(tx3, v21);
        v3 = Iterables.<JanusGraphVertex>getOnlyElement(v21.query().direction(Direction.OUT).labels("link").vertices());
        assertTrue(v3.isRemoved());
        tx3.commit();
    }


    /**
     * Tests multi-valued properties with special focus on indexing and incident unidirectional edges
     * which is not tested in {@link #testSchemaTypes()}
     * <p>
     * --&gt; TODO: split and move this into other test cases: ordering to query, indexing to index
     */
    @Test
    public <V> void testMultivaluedVertexProperty() {
        /*
         * Constant test data
         *
         * The values list below must have at least two elements. The string
         * literals were chosen arbitrarily and have no special significance.
         */
        final String foo = "foo", bar = "bar", weight = "weight";
        final List<String> values =
                ImmutableList.of("four", "score", "and", "seven");
        assertTrue("Values list must have multiple elements for this test to make sense",
                2 <= values.size());

        // Create property with name pname and a vertex
        PropertyKey w = makeKey(weight, Integer.class);
        PropertyKey f = ((StandardPropertyKeyMaker) mgmt.makePropertyKey(foo)).dataType(String.class).cardinality(Cardinality.LIST).sortKey(w).sortOrder(Order.DESC).make();
        mgmt.buildIndex(foo, Vertex.class).addKey(f).buildCompositeIndex();
        PropertyKey b = mgmt.makePropertyKey(bar).dataType(String.class).cardinality(Cardinality.LIST).make();
        mgmt.buildIndex(bar, Vertex.class).addKey(b).buildCompositeIndex();
        finishSchema();

        JanusGraphVertex v = tx.addVertex();

        // Insert prop values
        int i = 0;
        for (String s : values) {
            v.property(foo, s, weight, ++i);
            v.property(bar, s, weight, i);
        }

        //Verify correct number of properties
        assertCount(values.size(), v.properties(foo));
        assertCount(values.size(), v.properties(bar));
        //Verify order
        for (String prop : new String[]{foo, bar}) {
            int sum = 0;
            int index = values.size();
            for (Object o : v.query().labels(foo).properties()) {
                JanusGraphVertexProperty<String> p = (JanusGraphVertexProperty<String>) o;
                assertTrue(values.contains(p.value()));
                int wint = p.value(weight);
                sum += wint;
                if (prop == foo) assertEquals(index, wint);
                index--;
            }
            assertEquals(values.size() * (values.size() + 1) / 2, sum);
        }


        assertCount(1, tx.query().has(foo, values.get(1)).vertices());
        assertCount(1, tx.query().has(foo, values.get(3)).vertices());

        assertCount(1, tx.query().has(bar, values.get(1)).vertices());
        assertCount(1, tx.query().has(bar, values.get(3)).vertices());

        // Check that removing properties works
        asStream(v.properties(foo)).forEach(p -> p.remove());
        // Check that the properties were actually deleted from v
        assertEmpty(v.properties(foo));

        // Reopen database
        clopen();

        assertCount(0, tx.query().has(foo, values.get(1)).vertices());
        assertCount(0, tx.query().has(foo, values.get(3)).vertices());

        assertCount(1, tx.query().has(bar, values.get(1)).vertices());
        assertCount(1, tx.query().has(bar, values.get(3)).vertices());

        // Retrieve and check our test vertex
        v = getV(tx, v);
        assertEmpty(v.properties(foo));
        assertCount(values.size(), v.properties(bar));
        // Reinsert prop values
        for (String s : values) {
            v.property(foo, s);
        }
        assertCount(values.size(), v.properties(foo));

        // Check that removing properties works
        asStream(v.properties(foo)).forEach(p -> p.remove());
        // Check that the properties were actually deleted from v
        assertEmpty(v.properties(foo));
    }

    @Test
    public void testLocalGraphConfiguration() {
        setIllegalGraphOption(STORAGE_READONLY, ConfigOption.Type.LOCAL, true);
    }

    @Test
    public void testMaskableGraphConfig() {
        setAndCheckGraphOption(DB_CACHE, ConfigOption.Type.MASKABLE, true, false);
    }

    @Test
    public void testGlobalGraphConfig() {
        setAndCheckGraphOption(SYSTEM_LOG_TRANSACTIONS, ConfigOption.Type.GLOBAL, true, false);
    }

    @Test
    public void testGlobalOfflineGraphConfig() {
        setAndCheckGraphOption(DB_CACHE_TIME, ConfigOption.Type.GLOBAL_OFFLINE, 500L, 777L);
    }

    @Test
    public void testFixedGraphConfig() {
        setIllegalGraphOption(INITIAL_JANUSGRAPH_VERSION, ConfigOption.Type.FIXED, "foo");
    }

    @Test
    public void testManagedOptionMasking() throws BackendException {
        // Can't use clopen(...) for this test, because it's aware local vs global option types and
        // uses ManagementSystem where necessary.  We want to simulate an erroneous attempt to
        // override global options by tweaking the local config file (ignoring ManagementSystem),
        // so we have to bypass clopen(...).
        //clopen(
        //    option(ALLOW_STALE_CONFIG), false,
        //    option(ATTRIBUTE_ALLOW_ALL_SERIALIZABLE), false);

        // Check this test's assumptions about option default values

        Duration customCommitTime = Duration.ofMillis(456L);
        Preconditions.checkState(true == ALLOW_STALE_CONFIG.getDefaultValue());
        Preconditions.checkState(ALLOW_STALE_CONFIG.getType().equals(ConfigOption.Type.MASKABLE));
        Preconditions.checkState(!customCommitTime.equals(MAX_COMMIT_TIME.getDefaultValue()));

        // Disallow managed option masking and verify exception at graph startup
        close();
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(ALLOW_STALE_CONFIG), false);
        wc.set(ConfigElement.getPath(MAX_COMMIT_TIME), customCommitTime);
        try {
            graph = (StandardJanusGraph) JanusGraphFactory.open(wc);
            fail("Masking managed config options should be disabled in this configuration");
        } catch (JanusGraphConfigurationException e) {
            // Exception should cite the problematic setting's full name
            assertTrue(e.getMessage().contains(ConfigElement.getPath(MAX_COMMIT_TIME)));
        }

        // Allow managed option masking (default config again) and check that the local value is ignored and
        // that no exception is thrown
        close();
        wc = getConfiguration();
        wc.set(ConfigElement.getPath(ALLOW_STALE_CONFIG), true);
        wc.set(ConfigElement.getPath(MAX_COMMIT_TIME), customCommitTime);
        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);
        // Local value should be overridden by the default that already exists in the backend
        assertEquals(MAX_COMMIT_TIME.getDefaultValue(), graph.getConfiguration().getMaxCommitTime());

        // Wipe the storage backend
        graph.getBackend().clearStorage();
        try {
            graph.close();
        } catch (Throwable t) {
            log.debug("Swallowing throwable during shutdown after clearing backend storage", t);
        }

        // Bootstrap a new DB with managed option masking disabled
        wc = getConfiguration();
        wc.set(ConfigElement.getPath(ALLOW_STALE_CONFIG), false);
        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);
        close();

        // Check for expected exception
        wc = getConfiguration();
        wc.set(ConfigElement.getPath(MAX_COMMIT_TIME), customCommitTime);
        try {
            graph = (StandardJanusGraph) JanusGraphFactory.open(wc);
            fail("Masking managed config options should be disabled in this configuration");
        } catch (JanusGraphConfigurationException e) {
            // Exception should cite the problematic setting's full name
            assertTrue(e.getMessage().contains(ConfigElement.getPath(MAX_COMMIT_TIME)));
        }

        // Now check that ALLOW_STALE_CONFIG is actually MASKABLE -- enable it in the local config
        wc = getConfiguration();
        wc.set(ConfigElement.getPath(ALLOW_STALE_CONFIG), true);
        wc.set(ConfigElement.getPath(MAX_COMMIT_TIME), customCommitTime);
        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);
        // Local value should be overridden by the default that already exists in the backend
        assertEquals(MAX_COMMIT_TIME.getDefaultValue(), graph.getConfiguration().getMaxCommitTime());
    }

    @Test
    public void testTransactionConfiguration() {
        // Superficial tests for a few transaction builder methods

        // Test read-only transaction
        JanusGraphTransaction readOnlyTx = graph.buildTransaction().readOnly().start();
        try {
            readOnlyTx.addVertex();
            readOnlyTx.commit();
            fail("Read-only transactions should not be able to add a vertex and commit");
        } catch (Throwable t) {
            if (readOnlyTx.isOpen())
                readOnlyTx.rollback();
        }

        // Test custom log identifier
        String logID = "spam";
        StandardJanusGraphTx customLogIDTx = (StandardJanusGraphTx) graph.buildTransaction().logIdentifier(logID).start();
        assertEquals(logID, customLogIDTx.getConfiguration().getLogIdentifier());
        customLogIDTx.rollback();

        // Test timestamp
        Instant customTimestamp = Instant.ofEpochMilli(-42L);
        StandardJanusGraphTx customTimeTx = (StandardJanusGraphTx) graph.buildTransaction().commitTime(customTimestamp).start();
        assertTrue(customTimeTx.getConfiguration().hasCommitTime());
        assertEquals(customTimestamp, customTimeTx.getConfiguration().getCommitTime());
        customTimeTx.rollback();
    }

    private <T> void setAndCheckGraphOption(ConfigOption<T> opt, ConfigOption.Type requiredType, T firstValue, T secondValue) {
        // Sanity check: make sure the Type of the configoption is what we expect
        Preconditions.checkState(opt.getType().equals(requiredType));
        final EnumSet<ConfigOption.Type> allowedTypes =
                EnumSet.of(ConfigOption.Type.GLOBAL,
                        ConfigOption.Type.GLOBAL_OFFLINE,
                        ConfigOption.Type.MASKABLE);
        Preconditions.checkState(allowedTypes.contains(opt.getType()));

        // Sanity check: it's kind of pointless for the first and second values to be identical
        Preconditions.checkArgument(!firstValue.equals(secondValue));

        // Get full string path of config option
        final String path = ConfigElement.getPath(opt);

        // Set and check initial value before and after database restart
        mgmt.set(path, firstValue);
        assertEquals(firstValue.toString(), mgmt.get(path));
        // Close open tx first.  This is specific to BDB + GLOBAL_OFFLINE.
        // Basically: the BDB store manager throws a fit if shutdown is called
        // with one or more transactions still open, and GLOBAL_OFFLINE calls
        // shutdown on our behalf when we commit this change.
        tx.rollback();
        mgmt.commit();
        clopen();
        // Close tx again following clopen
        tx.rollback();
        assertEquals(firstValue.toString(), mgmt.get(path));

        // Set and check updated value before and after database restart
        mgmt.set(path, secondValue);
        assertEquals(secondValue.toString(), mgmt.get(path));
        mgmt.commit();
        clopen();
        tx.rollback();
        assertEquals(secondValue.toString(), mgmt.get(path));

        // Open a separate graph "g2"
        JanusGraph g2 = JanusGraphFactory.open(config);
        JanusGraphManagement m2 = g2.openManagement();
        assertEquals(secondValue.toString(), m2.get(path));

        // GLOBAL_OFFLINE options should be unmodifiable with g2 open
        if (opt.getType().equals(ConfigOption.Type.GLOBAL_OFFLINE)) {
            try {
                mgmt.set(path, firstValue);
                mgmt.commit();
                fail("Option " + path + " with type " + ConfigOption.Type.GLOBAL_OFFLINE + " should not be modifiable with concurrent instances");
            } catch (RuntimeException e) {
                log.debug("Caught expected exception", e);
            }
            assertEquals(secondValue.toString(), mgmt.get(path));
            // GLOBAL and MASKABLE should be modifiable even with g2 open
        } else {
            mgmt.set(path, firstValue);
            assertEquals(firstValue.toString(), mgmt.get(path));
            mgmt.commit();
            clopen();
            assertEquals(firstValue.toString(), mgmt.get(path));
        }

        m2.rollback();
        g2.close();
    }

    private <T> void setIllegalGraphOption(ConfigOption<T> opt, ConfigOption.Type requiredType, T attemptedValue) {
        // Sanity check: make sure the Type of the configoption is what we expect
        final ConfigOption.Type type = opt.getType();
        Preconditions.checkState(type.equals(requiredType));
        Preconditions.checkArgument(requiredType.equals(ConfigOption.Type.LOCAL) ||
                requiredType.equals(ConfigOption.Type.FIXED));

        // Get full string path of config option
        final String path = ConfigElement.getPath(opt);


        // Try to read the option
        try {
            mgmt.get(path);
        } catch (Throwable t) {
            log.debug("Caught expected exception", t);
        }

        // Try to modify the option
        try {
            mgmt.set(path, attemptedValue);
            mgmt.commit();
            fail("Option " + path + " with type " + type + " should not be modifiable in the persistent graph config");
        } catch (Throwable t) {
            log.debug("Caught expected exception", t);
        }
    }


   /* ==================================================================================
                            CONSISTENCY
     ==================================================================================*/

    /**
     * Tests the correct application of ConsistencyModifiers across transactional boundaries
     */
    @Test
    public void testConsistencyEnforcement() {
        PropertyKey uid = makeVertexIndexedUniqueKey("uid", Integer.class);
        PropertyKey name = makeKey("name", String.class);
        mgmt.setConsistency(uid, ConsistencyModifier.LOCK);
        mgmt.setConsistency(name, ConsistencyModifier.LOCK);
        mgmt.setConsistency(mgmt.getGraphIndex("uid"), ConsistencyModifier.LOCK);
        EdgeLabel knows = mgmt.makeEdgeLabel("knows").multiplicity(Multiplicity.SIMPLE).make();
        EdgeLabel spouse = mgmt.makeEdgeLabel("spouse").multiplicity(Multiplicity.ONE2ONE).make();
        EdgeLabel connect = mgmt.makeEdgeLabel("connect").multiplicity(Multiplicity.MULTI).make();
        EdgeLabel related = mgmt.makeEdgeLabel("related").multiplicity(Multiplicity.MULTI).make();
        mgmt.setConsistency(knows, ConsistencyModifier.LOCK);
        mgmt.setConsistency(spouse, ConsistencyModifier.LOCK);
        mgmt.setConsistency(related, ConsistencyModifier.FORK);
        finishSchema();

        name = tx.getPropertyKey("name");
        connect = tx.getEdgeLabel("connect");
        related = tx.getEdgeLabel("related");

        JanusGraphVertex v1 = tx.addVertex("uid", 1);
        JanusGraphVertex v2 = tx.addVertex("uid", 2);
        JanusGraphVertex v3 = tx.addVertex("uid", 3);

        Edge e1 = v1.addEdge(connect.name(), v2, name.name(), "e1");
        Edge e2 = v1.addEdge(related.name(), v2, name.name(), "e2");

        newTx();
        v1 = getV(tx, v1);
        /*
         ==== check fork, no fork behavior
         */
        long e1id = getId(e1);
        long e2id = getId(e2);
        e1 = Iterables.<JanusGraphEdge>getOnlyElement(v1.query().direction(Direction.OUT).labels("connect").edges());
        assertEquals("e1", e1.value("name"));
        assertEquals(e1id, getId(e1));
        e2 = Iterables.<JanusGraphEdge>getOnlyElement(v1.query().direction(Direction.OUT).labels("related").edges());
        assertEquals("e2", e2.value("name"));
        assertEquals(e2id, getId(e2));
        //Update edges - one should simply update, the other fork
        e1.property("name", "e1.2");
        e2.property("name", "e2.2");

        newTx();
        v1 = getV(tx, v1);

        e1 = Iterables.<JanusGraphEdge>getOnlyElement(v1.query().direction(Direction.OUT).labels("connect").edges());
        assertEquals("e1.2", e1.value("name"));
        assertEquals(e1id, getId(e1)); //should have same id
        e2 = Iterables.<JanusGraphEdge>getOnlyElement(v1.query().direction(Direction.OUT).labels("related").edges());
        assertEquals("e2.2", e2.value("name"));
        assertNotEquals(e2id, getId(e2)); //should have different id since forked

        clopen();

        /*
         === check cross transaction
         */
        final Random random = new Random();
        final long vids[] = {getId(v1), getId(v2), getId(v3)};
        //1) Index uniqueness
        executeLockConflictingTransactionJobs(graph, new TransactionJob() {
            private int pos = 0;

            @Override
            public void run(JanusGraphTransaction tx) {
                JanusGraphVertex u = getV(tx, vids[pos++]);
                u.property(VertexProperty.Cardinality.single, "uid", 5);
            }
        });
        //2) Property out-uniqueness
        executeLockConflictingTransactionJobs(graph, new TransactionJob() {
            @Override
            public void run(JanusGraphTransaction tx) {
                JanusGraphVertex u = getV(tx, vids[0]);
                u.property(VertexProperty.Cardinality.single, "name", "v" + random.nextInt(10));
            }
        });
        //3) knows simpleness
        executeLockConflictingTransactionJobs(graph, new TransactionJob() {
            @Override
            public void run(JanusGraphTransaction tx) {
                JanusGraphVertex u1 = getV(tx, vids[0]), u2 = getV(tx, vids[1]);
                u1.addEdge("knows", u2);
            }
        });
        //4) knows one2one (in 2 separate configurations)
        executeLockConflictingTransactionJobs(graph, new TransactionJob() {
            private int pos = 1;

            @Override
            public void run(JanusGraphTransaction tx) {
                JanusGraphVertex u1 = getV(tx, vids[0]),
                        u2 = getV(tx, vids[pos++]);
                u1.addEdge("spouse", u2);
            }
        });
        executeLockConflictingTransactionJobs(graph, new TransactionJob() {
            private int pos = 1;

            @Override
            public void run(JanusGraphTransaction tx) {
                JanusGraphVertex u1 = getV(tx, vids[pos++]),
                        u2 = getV(tx, vids[0]);
                u1.addEdge("spouse", u2);
            }
        });

        //######### TRY INVALID CONSISTENCY
        try {
            //Fork does not apply to constrained types
            mgmt.setConsistency(mgmt.getPropertyKey("name"), ConsistencyModifier.FORK);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    /**
     * A piece of logic to be executed in a transactional context
     */
    private static interface TransactionJob {
        public void run(JanusGraphTransaction tx);
    }

    /**
     * Executes a transaction job in two parallel transactions under the assumptions that the two transactions
     * should conflict and the one committed later should throw a locking exception.
     *
     * @param graph
     * @param job
     */
    private void executeLockConflictingTransactionJobs(JanusGraph graph, TransactionJob job) {
        JanusGraphTransaction tx1 = graph.newTransaction();
        JanusGraphTransaction tx2 = graph.newTransaction();
        job.run(tx1);
        job.run(tx2);
        /*
         * Under pessimistic locking, tx1 should abort and tx2 should commit.
         * Under optimistic locking, tx1 may commit and tx2 may abort.
         */
        if (isLockingOptimistic()) {
            tx1.commit();
            try {
                tx2.commit();
                fail("Storage backend does not abort conflicting transactions");
            } catch (JanusGraphException e) {
            }
        } else {
            try {
                tx1.commit();
                fail("Storage backend does not abort conflicting transactions");
            } catch (JanusGraphException e) {
            }
            tx2.commit();
        }
    }

    @Test
    public void testConcurrentConsistencyEnforcement() throws Exception {
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        JanusGraphIndex nameIndex = mgmt.buildIndex("name", Vertex.class)
                .addKey(name).unique().buildCompositeIndex();
        mgmt.setConsistency(nameIndex, ConsistencyModifier.LOCK);
        EdgeLabel married = mgmt.makeEdgeLabel("married").multiplicity(Multiplicity.ONE2ONE).make();
        mgmt.setConsistency(married, ConsistencyModifier.LOCK);
        mgmt.makeEdgeLabel("friend").multiplicity(Multiplicity.MULTI).make();
        finishSchema();

        JanusGraphVertex baseV = tx.addVertex("name", "base");
        newTx();
        final long baseVid = getId(baseV);
        final String nameA = "a", nameB = "b";
        final int parallelThreads = 4;

        int numSuccess = executeParallelTransactions(new TransactionJob() {
            @Override
            public void run(JanusGraphTransaction tx) {
                JanusGraphVertex a = tx.addVertex();
                JanusGraphVertex base = getV(tx, baseVid);
                base.addEdge("married", a);
            }
        }, parallelThreads);

        assertTrue("At most 1 tx should succeed: " + numSuccess, numSuccess <= 1);

        numSuccess = executeParallelTransactions(new TransactionJob() {
            @Override
            public void run(JanusGraphTransaction tx) {
                tx.addVertex("name", nameA);
                JanusGraphVertex b = tx.addVertex("name", nameB);
                b.addEdge("friend", b);
            }
        }, parallelThreads);

        newTx();
        long numA = Iterables.size(tx.query().has("name", nameA).vertices());
        long numB = Iterables.size(tx.query().has("name", nameB).vertices());
//        System.out.println(numA + " - " + numB);
        assertTrue("At most 1 tx should succeed: " + numSuccess, numSuccess <= 1);
        assertTrue(numA <= 1);
        assertTrue(numB <= 1);
    }

    private void failTransactionOnCommit(final TransactionJob job) {
        JanusGraphTransaction tx = graph.newTransaction();
        try {
            job.run(tx);
            tx.commit();
            fail();
        } catch (Exception e) {
            //e.printStackTrace();
        } finally {
            if (tx.isOpen()) tx.rollback();
        }
    }

    private int executeParallelTransactions(final TransactionJob job, int number) {
        final CountDownLatch startLatch = new CountDownLatch(number);
        final CountDownLatch finishLatch = new CountDownLatch(number);
        final AtomicInteger txSuccess = new AtomicInteger(0);
        for (int i = 0; i < number; i++) {
            new Thread() {
                @Override
                public void run() {
                    awaitAllThreadsReady();
                    JanusGraphTransaction tx = graph.newTransaction();
                    try {
                        job.run(tx);
                        tx.commit();
                        txSuccess.incrementAndGet();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        if (tx.isOpen()) tx.rollback();
                    } finally {
                        finishLatch.countDown();
                    }
                }

                private void awaitAllThreadsReady() {
                    startLatch.countDown();
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }.start();
        }

        try {
            finishLatch.await(10000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return txSuccess.get();
    }

   /* ==================================================================================
                            VERTEX CENTRIC QUERIES
     ==================================================================================*/

    @Test
    public void testVertexCentricQuery() {
        testVertexCentricQuery(10000 /*noVertices*/);
    }

    @SuppressWarnings("deprecation")
    public void testVertexCentricQuery(int noVertices) {
        makeVertexIndexedUniqueKey("name", String.class);
        PropertyKey time = makeKey("time", Integer.class);
        PropertyKey weight = makeKey("weight", Double.class);
        PropertyKey number = makeKey("number", Long.class);

        ((StandardEdgeLabelMaker) mgmt.makeEdgeLabel("connect")).sortKey(time).make();
        ((StandardEdgeLabelMaker) mgmt.makeEdgeLabel("connectDesc")).sortKey(time).sortOrder(Order.DESC).make();
        ((StandardEdgeLabelMaker) mgmt.makeEdgeLabel("friend")).sortKey(weight, time).sortOrder(Order.ASC).signature(number).make();
        ((StandardEdgeLabelMaker) mgmt.makeEdgeLabel("friendDesc")).sortKey(weight, time).sortOrder(Order.DESC).signature(number).make();
        ((StandardEdgeLabelMaker) mgmt.makeEdgeLabel("knows")).sortKey(number, weight).make();
        mgmt.makeEdgeLabel("follows").make();
        finishSchema();

        JanusGraphVertex v = tx.addVertex("name", "v");
        JanusGraphVertex u = tx.addVertex("name", "u");
        assertEquals(0, (noVertices - 1) % 3);
        JanusGraphVertex[] vs = new JanusGraphVertex[noVertices];
        for (int i = 1; i < noVertices; i++) {
            vs[i] = tx.addVertex("name", "v" + i);
        }
        EdgeLabel[] labelsV = {tx.getEdgeLabel("connect"), tx.getEdgeLabel("friend"), tx.getEdgeLabel("knows")};
        EdgeLabel[] labelsU = {tx.getEdgeLabel("connectDesc"), tx.getEdgeLabel("friendDesc"), tx.getEdgeLabel("knows")};
        for (int i = 1; i < noVertices; i++) {
            for (JanusGraphVertex vertex : new JanusGraphVertex[]{v, u}) {
                for (Direction d : new Direction[]{OUT, IN}) {
                    EdgeLabel label = vertex == v ? labelsV[i % 3] : labelsU[i % 3];
                    JanusGraphEdge e = d == OUT ? vertex.addEdge(n(label), vs[i]) :
                            vs[i].addEdge(n(label), vertex);
                    e.property("time", i);
                    e.property("weight", i % 4 + 0.5);
                    e.property("name", "e" + i);
                    e.property("number", i % 5);
                }
            }
        }
        int edgesPerLabel = noVertices / 3;


        VertexList vl;
        Map<JanusGraphVertex, Iterable<JanusGraphEdge>> results;
        Map<JanusGraphVertex, Iterable<JanusGraphVertexProperty>> results2;
        JanusGraphVertex[] qvs;
        int lastTime;
        Iterator<? extends Edge> outer;

        clopen();

        long[] vidsubset = new long[31 - 3];
        for (int i = 0; i < vidsubset.length; i++) vidsubset[i] = vs[i + 3].longId();
        Arrays.sort(vidsubset);

        //##################################################
        //Queries from Cache
        //##################################################
        clopen();
        for (int i = 1; i < noVertices; i++) vs[i] = getV(tx, vs[i].longId());
        v = getV(tx, v.longId());
        u = getV(tx, u.longId());
        qvs = new JanusGraphVertex[]{vs[6], vs[9], vs[12], vs[15], vs[60]};

        //To trigger queries from cache (don't copy!!!)
        assertCount(2 * (noVertices - 1), v.query().direction(Direction.BOTH).edges());


        assertEquals(1, v.query().propertyCount());

        assertEquals(10, size(v.query().labels("connect").limit(10).vertices()));
        assertEquals(10, size(u.query().labels("connectDesc").limit(10).vertices()));
        assertEquals(10, size(v.query().labels("connect").has("time", Cmp.GREATER_THAN, 30).limit(10).vertices()));
        assertEquals(10, size(u.query().labels("connectDesc").has("time", Cmp.GREATER_THAN, 30).limit(10).vertices()));

        lastTime = 0;
        for (JanusGraphEdge e : (Iterable<JanusGraphEdge>) v.query().labels("connect").direction(OUT).limit(20).edges()) {
            int nowTime = e.value("time");
            assertTrue(lastTime + " vs. " + nowTime, lastTime <= nowTime);
            lastTime = nowTime;
        }
        lastTime = Integer.MAX_VALUE;
        for (Edge e : (Iterable<JanusGraphEdge>) u.query().labels("connectDesc").direction(OUT).limit(20).edges()) {
            int nowTime = e.value("time");
            assertTrue(lastTime + " vs. " + nowTime, lastTime >= nowTime);
            lastTime = nowTime;
        }
        assertEquals(10, size(v.query().labels("connect").direction(OUT).has("time", Cmp.GREATER_THAN, 60).limit(10).vertices()));
        assertEquals(10, size(u.query().labels("connectDesc").direction(OUT).has("time", Cmp.GREATER_THAN, 60).limit(10).vertices()));

        outer = v.query().labels("connect").direction(OUT).limit(20).edges().iterator();
        for (Edge e : (Iterable<JanusGraphEdge>) v.query().labels("connect").direction(OUT).limit(10).edges()) {
            assertEquals(e, outer.next());
        }

        evaluateQuery(v.query().labels("connect").direction(OUT).interval("time", 3, 31), EDGE, 10, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("connect").direction(OUT).has("time", 15).has("weight", 3.5), EDGE, 1, 1, new boolean[]{false, true});
        evaluateQuery(u.query().labels("connectDesc").direction(OUT).interval("time", 3, 31), EDGE, 10, 1, new boolean[]{true, true});
        assertEquals(10, v.query().labels("connect").direction(IN).interval("time", 3, 31).edgeCount());
        assertEquals(10, u.query().labels("connectDesc").direction(IN).interval("time", 3, 31).edgeCount());
        assertEquals(0, v.query().labels("connect").direction(OUT).has("time", null).edgeCount());
        assertEquals(10, v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertexIds().size());
        assertEquals(edgesPerLabel - 10, v.query().labels("connect").direction(OUT).has("time", Cmp.GREATER_THAN, 31).count());
        assertEquals(10, size(v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertices()));
        assertEquals(3, v.query().labels("friend").direction(OUT).limit(3).count());
        evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", 0.5).limit(3), EDGE, 3, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", 0.5), EDGE, 3, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", Contain.IN, ImmutableList.of(0.5)), EDGE, 3, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", Contain.IN, ImmutableList.of(0.5, 1.5, 2.5)).interval("time", 3, 33), EDGE, 7, 3, new boolean[]{true, true});
        int friendsWhoHaveOutEdgesWithWeightBetweenPointFiveAndOnePointFive = (int) Math.round(Math.ceil(1667 * noVertices / 10000.0));
        evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", Contain.IN, ImmutableList.of(0.5, 1.5)), EDGE,
            friendsWhoHaveOutEdgesWithWeightBetweenPointFiveAndOnePointFive, 2, new boolean[]{true, true});
        assertEquals(3, u.query().labels("friendDesc").direction(OUT).interval("time", 3, 33).has("weight", 0.5).edgeCount());
        assertEquals(1, v.query().labels("friend").direction(OUT).has("weight", 0.5).interval("time", 4, 10).edgeCount());
        assertEquals(1, u.query().labels("friendDesc").direction(OUT).has("weight", 0.5).interval("time", 4, 10).edgeCount());
        assertEquals(3, v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", 0.5).edgeCount());
        assertEquals(4, v.query().labels("friend").direction(OUT).has("time", Cmp.LESS_THAN_EQUAL, 10).edgeCount());
        assertEquals(edgesPerLabel - 4, v.query().labels("friend").direction(OUT).has("time", Cmp.GREATER_THAN, 10).edgeCount());
        assertEquals(20, v.query().labels("friend", "connect").direction(OUT).interval("time", 3, 33).edgeCount());

        assertEquals((int) Math.ceil(edgesPerLabel / 5.0), v.query().labels("knows").direction(OUT).has("number", 0).edgeCount());
        assertEquals((int) Math.ceil(edgesPerLabel / 5.0), v.query().labels("knows").direction(OUT).has("number", 0).interval("weight", 0.0, 4.0).edgeCount());
        assertEquals((int) Math.ceil(edgesPerLabel / (5.0 * 2)), v.query().labels("knows").direction(OUT).has("number", 0).interval("weight", 0.0, 2.0).edgeCount());
        assertEquals((int) Math.floor(edgesPerLabel / (5.0 * 2)), v.query().labels("knows").direction(OUT).has("number", 0).interval("weight", 2.1, 4.0).edgeCount());
        assertEquals(20, size(v.query().labels("connect", "friend").direction(OUT).interval("time", 3, 33).vertices()));
        assertEquals(20, size(v.query().labels("connect", "friend").direction(OUT).interval("time", 3, 33).vertexIds()));
        assertEquals(30, v.query().labels("friend", "connect", "knows").direction(OUT).interval("time", 3, 33).edgeCount());
        assertEquals(noVertices - 2, v.query().labels("friend", "connect", "knows").direction(OUT).has("time", Cmp.NOT_EQUAL, 10).edgeCount());

        assertEquals(0, v.query().has("age", null).labels("undefined").direction(OUT).edgeCount());
        assertEquals(1, v.query().labels("connect").direction(OUT).adjacent(vs[6]).has("time", 6).edgeCount());
        assertEquals(1, v.query().labels("knows").direction(OUT).adjacent(vs[11]).edgeCount());
        assertEquals(1, v.query().labels("knows").direction(IN).adjacent(vs[11]).edgeCount());
        assertEquals(2, v.query().labels("knows").direction(BOTH).adjacent(vs[11]).edgeCount());
        assertEquals(1, v.query().labels("knows").direction(OUT).adjacent(vs[11]).has("weight", 3.5).edgeCount());
        assertEquals(2, v.query().labels("connect").adjacent(vs[6]).has("time", 6).edgeCount());
        assertEquals(0, v.query().labels("connect").adjacent(vs[8]).has("time", 8).edgeCount());

        assertEquals(edgesPerLabel, v.query().labels("connect").direction(OUT).edgeCount());
        assertEquals(edgesPerLabel, v.query().labels("connect").direction(IN).edgeCount());
        assertEquals(2 * edgesPerLabel, v.query().labels("connect").direction(BOTH).edgeCount());

        assertEquals(edgesPerLabel, v.query().labels("connect").has("undefined", null).direction(OUT).edgeCount());
        assertEquals(2 * (int) Math.ceil((noVertices - 1) / 4.0), size(v.query().labels("connect", "friend", "knows").has("weight", 1.5).vertexIds()));
        assertEquals(1, v.query().direction(IN).has("time", 1).edgeCount());
        assertEquals(10, v.query().direction(OUT).interval("time", 4, 14).edgeCount());
        assertEquals(9, v.query().direction(IN).interval("time", 4, 14).has("time", Cmp.NOT_EQUAL, 10).edgeCount());
        assertEquals(9, v.query().direction(OUT).interval("time", 4, 14).has("time", Cmp.NOT_EQUAL, 10).edgeCount());
        assertEquals(noVertices - 1, size(v.query().direction(OUT).vertices()));
        assertEquals(noVertices - 1, size(v.query().direction(IN).vertices()));
        for (Direction dir : new Direction[]{IN, OUT}) {
            vl = v.query().labels().direction(dir).interval("time", 3, 31).vertexIds();
            vl.sort();
            for (int i = 0; i < vl.size(); i++) assertEquals(vidsubset[i], vl.getID(i));
        }
        assertCount(2 * (noVertices - 1), v.query().direction(Direction.BOTH).edges());


        //Property queries
        assertEquals(1, size(v.query().properties()));
        assertEquals(1, size(v.query().keys("name").properties()));

        //MultiQueries
        results = tx.multiQuery(qvs).direction(IN).labels("connect").edges();
        for (Iterable<JanusGraphEdge> result : results.values()) assertEquals(1, size(result));
        results = tx.multiQuery(Sets.newHashSet(qvs)).labels("connect").edges();
        for (Iterable<JanusGraphEdge> result : results.values()) assertEquals(2, size(result));
        results = tx.multiQuery(qvs).labels("knows").edges();
        for (Iterable<JanusGraphEdge> result : results.values()) assertEquals(0, size(result));
        results = tx.multiQuery(qvs).edges();
        for (Iterable<JanusGraphEdge> result : results.values()) assertEquals(4, size(result));
        results2 = tx.multiQuery(qvs).properties();
        for (Iterable<JanusGraphVertexProperty> result : results2.values()) assertEquals(1, size(result));
        results2 = tx.multiQuery(qvs).keys("name").properties();
        for (Iterable<JanusGraphVertexProperty> result : results2.values()) assertEquals(1, size(result));

        //##################################################
        //Same queries as above but without memory loading (i.e. omitting the first query)
        //##################################################
        clopen();
        for (int i = 1; i < noVertices; i++) vs[i] = getV(tx, vs[i].longId());
        v = getV(tx, v.longId());
        u = getV(tx, u.longId());
        qvs = new JanusGraphVertex[]{vs[6], vs[9], vs[12], vs[15], vs[60]};

        assertEquals(10, size(v.query().labels("connect").limit(10).vertices()));
        assertEquals(10, size(u.query().labels("connectDesc").limit(10).vertices()));
        assertEquals(10, size(v.query().labels("connect").has("time", Cmp.GREATER_THAN, 30).limit(10).vertices()));
        assertEquals(10, size(u.query().labels("connectDesc").has("time", Cmp.GREATER_THAN, 30).limit(10).vertices()));

        lastTime = 0;
        for (Edge e : (Iterable<JanusGraphEdge>) v.query().labels("connect").direction(OUT).limit(20).edges()) {
            int nowTime = e.value("time");
            assertTrue(lastTime + " vs. " + nowTime, lastTime <= nowTime);
            lastTime = nowTime;
        }
        lastTime = Integer.MAX_VALUE;
        for (Edge e : (Iterable<JanusGraphEdge>) u.query().labels("connectDesc").direction(OUT).limit(20).edges()) {
            int nowTime = e.value("time");
            assertTrue(lastTime + " vs. " + nowTime, lastTime >= nowTime);
            lastTime = nowTime;
        }
        assertEquals(10, size(v.query().labels("connect").direction(OUT).has("time", Cmp.GREATER_THAN, 60).limit(10).vertices()));
        assertEquals(10, size(u.query().labels("connectDesc").direction(OUT).has("time", Cmp.GREATER_THAN, 60).limit(10).vertices()));

        outer = v.query().labels("connect").direction(OUT).limit(20).edges().iterator();
        for (Edge e : (Iterable<JanusGraphEdge>) v.query().labels("connect").direction(OUT).limit(10).edges()) {
            assertEquals(e, outer.next());
        }

        evaluateQuery(v.query().labels("connect").direction(OUT).interval("time", 3, 31), EDGE, 10, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("connect").direction(OUT).has("time", 15).has("weight", 3.5), EDGE, 1, 1, new boolean[]{false, true});
        evaluateQuery(u.query().labels("connectDesc").direction(OUT).interval("time", 3, 31), EDGE, 10, 1, new boolean[]{true, true});
        assertEquals(10, v.query().labels("connect").direction(IN).interval("time", 3, 31).edgeCount());
        assertEquals(10, u.query().labels("connectDesc").direction(IN).interval("time", 3, 31).edgeCount());
        assertEquals(0, v.query().labels("connect").direction(OUT).has("time", null).edgeCount());
        assertEquals(10, v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertexIds().size());
        assertEquals(edgesPerLabel - 10, v.query().labels("connect").direction(OUT).has("time", Cmp.GREATER_THAN, 31).count());
        assertEquals(10, size(v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertices()));
        assertEquals(3, v.query().labels("friend").direction(OUT).limit(3).count());
        evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", 0.5).limit(3), EDGE, 3, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", 0.5), EDGE, 3, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", Contain.IN, ImmutableList.of(0.5)), EDGE, 3, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", Contain.IN, ImmutableList.of(0.5, 1.5, 2.5)).interval("time", 3, 33), EDGE, 7, 3, new boolean[]{true, true});
        evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", Contain.IN, ImmutableList.of(0.5, 1.5)), EDGE,
            friendsWhoHaveOutEdgesWithWeightBetweenPointFiveAndOnePointFive, 2, new boolean[]{true, true});
        assertEquals(3, u.query().labels("friendDesc").direction(OUT).interval("time", 3, 33).has("weight", 0.5).edgeCount());
        assertEquals(1, v.query().labels("friend").direction(OUT).has("weight", 0.5).interval("time", 4, 10).edgeCount());
        assertEquals(1, u.query().labels("friendDesc").direction(OUT).has("weight", 0.5).interval("time", 4, 10).edgeCount());
        assertEquals(3, v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", 0.5).edgeCount());
        assertEquals(4, v.query().labels("friend").direction(OUT).has("time", Cmp.LESS_THAN_EQUAL, 10).edgeCount());
        assertEquals(edgesPerLabel - 4, v.query().labels("friend").direction(OUT).has("time", Cmp.GREATER_THAN, 10).edgeCount());
        assertEquals(20, v.query().labels("friend", "connect").direction(OUT).interval("time", 3, 33).edgeCount());

        assertEquals((int) Math.ceil(edgesPerLabel / 5.0), v.query().labels("knows").direction(OUT).has("number", 0).edgeCount());
        assertEquals((int) Math.ceil(edgesPerLabel / 5.0), v.query().labels("knows").direction(OUT).has("number", 0).interval("weight", 0.0, 4.0).edgeCount());
        assertEquals((int) Math.ceil(edgesPerLabel / (5.0 * 2)), v.query().labels("knows").direction(OUT).has("number", 0).interval("weight", 0.0, 2.0).edgeCount());
        assertEquals((int) Math.floor(edgesPerLabel / (5.0 * 2)), v.query().labels("knows").direction(OUT).has("number", 0).interval("weight", 2.1, 4.0).edgeCount());
        assertEquals(20, size(v.query().labels("connect", "friend").direction(OUT).interval("time", 3, 33).vertices()));
        assertEquals(20, size(v.query().labels("connect", "friend").direction(OUT).interval("time", 3, 33).vertexIds()));
        assertEquals(30, v.query().labels("friend", "connect", "knows").direction(OUT).interval("time", 3, 33).edgeCount());
        assertEquals(noVertices - 2, v.query().labels("friend", "connect", "knows").direction(OUT).has("time", Cmp.NOT_EQUAL, 10).edgeCount());

        assertEquals(0, v.query().has("age", null).labels("undefined").direction(OUT).edgeCount());
        assertEquals(1, v.query().labels("connect").direction(OUT).adjacent(vs[6]).has("time", 6).edgeCount());
        assertEquals(1, v.query().labels("knows").direction(OUT).adjacent(vs[11]).edgeCount());
        assertEquals(1, v.query().labels("knows").direction(IN).adjacent(vs[11]).edgeCount());
        assertEquals(2, v.query().labels("knows").direction(BOTH).adjacent(vs[11]).edgeCount());
        assertEquals(1, v.query().labels("knows").direction(OUT).adjacent(vs[11]).has("weight", 3.5).edgeCount());
        assertEquals(2, v.query().labels("connect").adjacent(vs[6]).has("time", 6).edgeCount());
        assertEquals(0, v.query().labels("connect").adjacent(vs[8]).has("time", 8).edgeCount());

        assertEquals(edgesPerLabel, v.query().labels("connect").direction(OUT).edgeCount());
        assertEquals(edgesPerLabel, v.query().labels("connect").direction(IN).edgeCount());
        assertEquals(2 * edgesPerLabel, v.query().labels("connect").direction(BOTH).edgeCount());

        assertEquals(edgesPerLabel, v.query().labels("connect").has("undefined", null).direction(OUT).edgeCount());
        assertEquals(2 * (int) Math.ceil((noVertices - 1) / 4.0), size(v.query().labels("connect", "friend", "knows").has("weight", 1.5).vertexIds()));
        assertEquals(1, v.query().direction(IN).has("time", 1).edgeCount());
        assertEquals(10, v.query().direction(OUT).interval("time", 4, 14).edgeCount());
        assertEquals(9, v.query().direction(IN).interval("time", 4, 14).has("time", Cmp.NOT_EQUAL, 10).edgeCount());
        assertEquals(9, v.query().direction(OUT).interval("time", 4, 14).has("time", Cmp.NOT_EQUAL, 10).edgeCount());
        assertEquals(noVertices - 1, size(v.query().direction(OUT).vertices()));
        assertEquals(noVertices - 1, size(v.query().direction(IN).vertices()));
        for (Direction dir : new Direction[]{IN, OUT}) {
            vl = v.query().labels().direction(dir).interval("time", 3, 31).vertexIds();
            vl.sort();
            for (int i = 0; i < vl.size(); i++) assertEquals(vidsubset[i], vl.getID(i));
        }
        assertCount(2 * (noVertices - 1), v.query().direction(Direction.BOTH).edges());


        //Property queries
        assertEquals(1, size(v.query().properties()));
        assertEquals(1, size(v.query().keys("name").properties()));

        //MultiQueries
        results = tx.multiQuery(qvs).direction(IN).labels("connect").edges();
        for (Iterable<JanusGraphEdge> result : results.values()) assertEquals(1, size(result));
        results = tx.multiQuery(Sets.newHashSet(qvs)).labels("connect").edges();
        for (Iterable<JanusGraphEdge> result : results.values()) assertEquals(2, size(result));
        results = tx.multiQuery(qvs).labels("knows").edges();
        for (Iterable<JanusGraphEdge> result : results.values()) assertEquals(0, size(result));
        results = tx.multiQuery(qvs).edges();
        for (Iterable<JanusGraphEdge> result : results.values()) assertEquals(4, size(result));
        results2 = tx.multiQuery(qvs).properties();
        for (Iterable<JanusGraphVertexProperty> result : results2.values()) assertEquals(1, size(result));
        results2 = tx.multiQuery(qvs).keys("name").properties();
        for (Iterable<JanusGraphVertexProperty> result : results2.values()) assertEquals(1, size(result));

        //##################################################
        //End copied queries
        //##################################################

        newTx();

        v = Iterables.<JanusGraphVertex>getOnlyElement(tx.query().has("name", "v").vertices());
        assertNotNull(v);
        assertEquals(2, v.query().has("weight", 1.5).interval("time", 10, 30).limit(2).vertexIds().size());
        assertEquals(10, v.query().has("weight", 1.5).interval("time", 10, 30).vertexIds().size());

        newTx();

        v = Iterables.<JanusGraphVertex>getOnlyElement(tx.query().has("name", "v").vertices());
        assertNotNull(v);
        assertEquals(2, v.query().has("weight", 1.5).interval("time", 10, 30).limit(2).edgeCount());
        assertEquals(10, v.query().has("weight", 1.5).interval("time", 10, 30).edgeCount());


        newTx();
        //Test partially new vertex queries
        JanusGraphVertex[] qvs2 = new JanusGraphVertex[qvs.length + 2];
        qvs2[0] = tx.addVertex();
        for (int i = 0; i < qvs.length; i++) qvs2[i + 1] = getV(tx, qvs[i].longId());
        qvs2[qvs2.length - 1] = tx.addVertex();
        qvs2[0].addEdge("connect", qvs2[qvs2.length - 1]);
        qvs2[qvs2.length - 1].addEdge("connect", qvs2[0]);
        results = tx.multiQuery(qvs2).direction(IN).labels("connect").edges();
        for (Iterable<JanusGraphEdge> result : results.values()) assertEquals(1, size(result));

    }

    @Test
    public void testRelationTypeIndexes() {
        PropertyKey weight = makeKey("weight", Float.class);
        PropertyKey time = makeKey("time", Long.class);

        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.LIST).make();
        EdgeLabel connect = mgmt.makeEdgeLabel("connect").signature(time).make();
        EdgeLabel child = mgmt.makeEdgeLabel("child").multiplicity(Multiplicity.ONE2MANY).make();
        EdgeLabel link = mgmt.makeEdgeLabel("link").unidirected().make();

        RelationTypeIndex name1 = mgmt.buildPropertyIndex(name, "weightDesc", weight);

        RelationTypeIndex connect1 = mgmt.buildEdgeIndex(connect, "weightAsc", Direction.BOTH, incr, weight);
        RelationTypeIndex connect2 = mgmt.buildEdgeIndex(connect, "weightDesc", Direction.OUT, decr, weight);
        RelationTypeIndex connect3 = mgmt.buildEdgeIndex(connect, "time+weight", Direction.OUT, decr, time, weight);

        RelationTypeIndex child1 = mgmt.buildEdgeIndex(child, "time", Direction.OUT, time);

        RelationTypeIndex link1 = mgmt.buildEdgeIndex(link, "time", Direction.OUT, time);

        final String name1n = name1.name(), connect1n = connect1.name(), connect2n = connect2.name(),
                connect3n = connect3.name(), child1n = child1.name(), link1n = link1.name();

        // ########### INSPECTION & FAILURE ##############

        assertTrue(mgmt.containsRelationIndex(name, "weightDesc"));
        assertTrue(mgmt.containsRelationIndex(connect, "weightDesc"));
        assertFalse(mgmt.containsRelationIndex(child, "weightDesc"));
        assertEquals("time+weight", mgmt.getRelationIndex(connect, "time+weight").name());
        assertNotNull(mgmt.getRelationIndex(link, "time"));
        assertNull(mgmt.getRelationIndex(name, "time"));
        assertEquals(1, size(mgmt.getRelationIndexes(child)));
        assertEquals(3, size(mgmt.getRelationIndexes(connect)));
        assertEquals(0, size(mgmt.getRelationIndexes(weight)));
        try {
            //Name already exists
            mgmt.buildEdgeIndex(connect, "weightAsc", Direction.OUT, time);
            fail();
        } catch (SchemaViolationException e) {
        }
//        try {
//           //Invalid key - must be single valued
//           mgmt.createEdgeIndex(connect,"blablub",Direction.OUT,name);
//           fail();
//        } catch (IllegalArgumentException e) {}
        try {
            //Not valid in this direction due to multiplicity constraint
            mgmt.buildEdgeIndex(child, "blablub", Direction.IN, time);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //Not valid in this direction due to unidirectionality
            mgmt.buildEdgeIndex(link, "blablub", Direction.BOTH, time);
            fail();
        } catch (IllegalArgumentException e) {
        }

        // ########## END INSPECTION ###########

        finishSchema();

        weight = mgmt.getPropertyKey("weight");
        time = mgmt.getPropertyKey("time");

        name = mgmt.getPropertyKey("name");
        connect = mgmt.getEdgeLabel("connect");
        child = mgmt.getEdgeLabel("child");
        link = mgmt.getEdgeLabel("link");

        // ########### INSPECTION & FAILURE (copied from above) ##############

        assertTrue(mgmt.containsRelationIndex(name, "weightDesc"));
        assertTrue(mgmt.containsRelationIndex(connect, "weightDesc"));
        assertFalse(mgmt.containsRelationIndex(child, "weightDesc"));
        assertEquals("time+weight", mgmt.getRelationIndex(connect, "time+weight").name());
        assertNotNull(mgmt.getRelationIndex(link, "time"));
        assertNull(mgmt.getRelationIndex(name, "time"));
        assertEquals(1, Iterables.size(mgmt.getRelationIndexes(child)));
        assertEquals(3, Iterables.size(mgmt.getRelationIndexes(connect)));
        assertEquals(0, Iterables.size(mgmt.getRelationIndexes(weight)));
        try {
            //Name already exists
            mgmt.buildEdgeIndex(connect, "weightAsc", Direction.OUT, time);
            fail();
        } catch (SchemaViolationException e) {
        }
//        try {
//           //Invalid key - must be single valued
//           mgmt.createEdgeIndex(connect,"blablub",Direction.OUT,name);
//           fail();
//        } catch (IllegalArgumentException e) {}
        try {
            //Not valid in this direction due to multiplicity constraint
            mgmt.buildEdgeIndex(child, "blablub", Direction.IN, time);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //Not valid in this direction due to unidirectionality
            mgmt.buildEdgeIndex(link, "blablub", Direction.BOTH, time);
            fail();
        } catch (IllegalArgumentException e) {
        }

        // ########## END INSPECTION ###########

        mgmt.rollback();

        /*
        ########## TEST WITHIN TRANSACTION ##################
        */

        weight = tx.getPropertyKey("weight");
        time = tx.getPropertyKey("time");

        final int numV = 100;
        JanusGraphVertex v = tx.addVertex();
        JanusGraphVertex ns[] = new JanusGraphVertex[numV];

        for (int i = 0; i < numV; i++) {
            double w = (i * 0.5) % 5;
            long t = (i + 77) % numV;
            VertexProperty p = v.property("name", "v" + i, "weight", w, "time", t);

            ns[i] = tx.addVertex();
            for (String label : new String[]{"connect", "child", "link"}) {
                Edge e = v.addEdge(label, ns[i], "weight", w, "time", t);
            }
        }
        JanusGraphVertex u = ns[0];
        VertexList vl;

        //######### QUERIES ##########
        v = getV(tx, v);
        u = getV(tx, u);

        evaluateQuery(v.query().keys("name").has("weight", Cmp.GREATER_THAN, 3.6),
                PROPERTY, 2 * numV / 10, 1, new boolean[]{false, true});
        evaluateQuery(v.query().keys("name").has("weight", Cmp.LESS_THAN, 0.9).orderBy("weight", incr),
                PROPERTY, 2 * numV / 10, 1, new boolean[]{true, true}, weight, Order.ASC);
        evaluateQuery(v.query().keys("name").interval("weight", 1.1, 2.2).orderBy("weight", decr).limit(numV / 10),
                PROPERTY, numV / 10, 1, new boolean[]{true, false}, weight, Order.DESC);
        evaluateQuery(v.query().keys("name").has("time", Cmp.EQUAL, 5).orderBy("weight", decr),
                PROPERTY, 1, 1, new boolean[]{false, false}, weight, Order.DESC);
        evaluateQuery(v.query().keys("name"),
                PROPERTY, numV, 1, new boolean[]{true, true});

        evaluateQuery(v.query().labels("child").direction(OUT).has("time", Cmp.EQUAL, 5),
                EDGE, 1, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("child").direction(BOTH).has("time", Cmp.EQUAL, 5),
                EDGE, 1, 2, new boolean[0]);
        evaluateQuery(v.query().labels("child").direction(OUT).interval("time", 10, 20).orderBy("weight", decr).limit(5),
                EDGE, 5, 1, new boolean[]{true, false}, weight, Order.DESC);
        evaluateQuery(v.query().labels("child").direction(BOTH).interval("weight", 0.0, 1.0).orderBy("weight", decr),
                EDGE, 2 * numV / 10, 2, new boolean[]{false, false}, weight, Order.DESC);
        evaluateQuery(v.query().labels("child").direction(OUT).interval("weight", 0.0, 1.0),
                EDGE, 2 * numV / 10, 1, new boolean[]{false, true});
        evaluateQuery(v.query().labels("child").direction(BOTH),
                EDGE, numV, 1, new boolean[]{true, true});
        vl = v.query().labels("child").direction(BOTH).vertexIds();
        assertEquals(numV, vl.size());
        assertTrue(vl.isSorted());
        assertTrue(isSortedByID(vl));
        evaluateQuery(v.query().labels("child").interval("weight", 0.0, 1.0).direction(OUT),
                EDGE, 2 * numV / 10, 1, new boolean[]{false, true});
        vl = v.query().labels("child").interval("weight", 0.0, 1.0).direction(OUT).vertexIds();
        assertEquals(2 * numV / 10, vl.size());
        assertTrue(vl.isSorted());
        assertTrue(isSortedByID(vl));
        evaluateQuery(v.query().labels("child").interval("time", 70, 80).direction(OUT).orderBy("time", incr),
                EDGE, 10, 1, new boolean[]{true, true}, time, Order.ASC);
        vl = v.query().labels("child").interval("time", 70, 80).direction(OUT).orderBy("time", incr).vertexIds();
        assertEquals(10, vl.size());
        assertFalse(vl.isSorted());
        assertFalse(isSortedByID(vl));
        vl.sort();
        assertTrue(vl.isSorted());
        assertTrue(isSortedByID(vl));

        evaluateQuery(v.query().labels("connect").has("time", Cmp.EQUAL, 5).interval("weight", 0.0, 5.0).direction(OUT),
                EDGE, 1, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("connect").has("time", Cmp.EQUAL, 5).interval("weight", 0.0, 5.0).direction(BOTH),
                EDGE, 1, 2, new boolean[0]);
        evaluateQuery(v.query().labels("connect").interval("time", 10, 20).interval("weight", 0.0, 5.0).direction(OUT),
                EDGE, 10, 1, new boolean[]{false, true});
        evaluateQuery(v.query().labels("connect").direction(OUT).orderBy("weight", incr).limit(10),
                EDGE, 10, 1, new boolean[]{true, true}, weight, Order.ASC);
        evaluateQuery(v.query().labels("connect").direction(OUT).orderBy("weight", decr).limit(10),
                EDGE, 10, 1, new boolean[]{true, true}, weight, Order.DESC);
        evaluateQuery(v.query().labels("connect").direction(OUT).interval("weight", 1.4, 2.75).orderBy("weight", decr),
                EDGE, 3 * numV / 10, 1, new boolean[]{true, true}, weight, Order.DESC);
        evaluateQuery(v.query().labels("connect").direction(OUT).has("time", Cmp.EQUAL, 22).orderBy("weight", decr),
                EDGE, 1, 1, new boolean[]{true, true}, weight, Order.DESC);
        evaluateQuery(v.query().labels("connect").direction(OUT).has("time", Cmp.EQUAL, 22).orderBy("weight", incr),
                EDGE, 1, 1, new boolean[]{true, false}, weight, Order.ASC);
        evaluateQuery(v.query().labels("connect").direction(OUT).adjacent(u),
                EDGE, 1, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("connect").direction(OUT).has("weight", Cmp.EQUAL, 0.0).adjacent(u),
                EDGE, 1, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("connect").direction(OUT).interval("weight", 0.0, 1.0).adjacent(u),
                EDGE, 1, 1, new boolean[]{false, true});
        evaluateQuery(v.query().labels("connect").direction(OUT).interval("time", 50, 100).adjacent(u),
                EDGE, 1, 1, new boolean[]{false, true});

        evaluateQuery(v.query(),
                RELATION, numV * 4, 1, new boolean[]{true, true});
        evaluateQuery(v.query().direction(OUT),
                RELATION, numV * 4, 1, new boolean[]{false, true});

        //--------------

        clopen();

        weight = tx.getPropertyKey("weight");
        time = tx.getPropertyKey("time");

        //######### QUERIES (copied from above) ##########
        v = getV(tx, v);
        u = getV(tx, u);

        evaluateQuery(v.query().keys("name").has("weight", Cmp.GREATER_THAN, 3.6),
                PROPERTY, 2 * numV / 10, 1, new boolean[]{false, true});
        evaluateQuery(v.query().keys("name").has("weight", Cmp.LESS_THAN, 0.9).orderBy("weight", incr),
                PROPERTY, 2 * numV / 10, 1, new boolean[]{true, true}, weight, Order.ASC);
        evaluateQuery(v.query().keys("name").interval("weight", 1.1, 2.2).orderBy("weight", decr).limit(numV / 10),
                PROPERTY, numV / 10, 1, new boolean[]{true, false}, weight, Order.DESC);
        evaluateQuery(v.query().keys("name").has("time", Cmp.EQUAL, 5).orderBy("weight", decr),
                PROPERTY, 1, 1, new boolean[]{false, false}, weight, Order.DESC);
        evaluateQuery(v.query().keys("name"),
                PROPERTY, numV, 1, new boolean[]{true, true});

        evaluateQuery(v.query().labels("child").direction(OUT).has("time", Cmp.EQUAL, 5),
                EDGE, 1, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("child").direction(BOTH).has("time", Cmp.EQUAL, 5),
                EDGE, 1, 2, new boolean[0]);
        evaluateQuery(v.query().labels("child").direction(OUT).interval("time", 10, 20).orderBy("weight", decr).limit(5),
                EDGE, 5, 1, new boolean[]{true, false}, weight, Order.DESC);
        evaluateQuery(v.query().labels("child").direction(BOTH).interval("weight", 0.0, 1.0).orderBy("weight", decr),
                EDGE, 2 * numV / 10, 2, new boolean[]{false, false}, weight, Order.DESC);
        evaluateQuery(v.query().labels("child").direction(OUT).interval("weight", 0.0, 1.0),
                EDGE, 2 * numV / 10, 1, new boolean[]{false, true});
        evaluateQuery(v.query().labels("child").direction(BOTH),
                EDGE, numV, 1, new boolean[]{true, true});
        vl = v.query().labels("child").direction(BOTH).vertexIds();
        assertEquals(numV, vl.size());
        assertTrue(vl.isSorted());
        assertTrue(isSortedByID(vl));
        evaluateQuery(v.query().labels("child").interval("weight", 0.0, 1.0).direction(OUT),
                EDGE, 2 * numV / 10, 1, new boolean[]{false, true});
        vl = v.query().labels("child").interval("weight", 0.0, 1.0).direction(OUT).vertexIds();
        assertEquals(2 * numV / 10, vl.size());
        assertTrue(vl.isSorted());
        assertTrue(isSortedByID(vl));
        evaluateQuery(v.query().labels("child").interval("time", 70, 80).direction(OUT).orderBy("time", incr),
                EDGE, 10, 1, new boolean[]{true, true}, time, Order.ASC);
        vl = v.query().labels("child").interval("time", 70, 80).direction(OUT).orderBy("time", incr).vertexIds();
        assertEquals(10, vl.size());
        assertFalse(vl.isSorted());
        assertFalse(isSortedByID(vl));
        vl.sort();
        assertTrue(vl.isSorted());
        assertTrue(isSortedByID(vl));

        evaluateQuery(v.query().labels("connect").has("time", Cmp.EQUAL, 5).interval("weight", 0.0, 5.0).direction(OUT),
                EDGE, 1, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("connect").has("time", Cmp.EQUAL, 5).interval("weight", 0.0, 5.0).direction(BOTH),
                EDGE, 1, 2, new boolean[0]);
        evaluateQuery(v.query().labels("connect").interval("time", 10, 20).interval("weight", 0.0, 5.0).direction(OUT),
                EDGE, 10, 1, new boolean[]{false, true});
        evaluateQuery(v.query().labels("connect").direction(OUT).orderBy("weight", incr).limit(10),
                EDGE, 10, 1, new boolean[]{true, true}, weight, Order.ASC);
        evaluateQuery(v.query().labels("connect").direction(OUT).orderBy("weight", decr).limit(10),
                EDGE, 10, 1, new boolean[]{true, true}, weight, Order.DESC);
        evaluateQuery(v.query().labels("connect").direction(OUT).interval("weight", 1.4, 2.75).orderBy("weight", decr),
                EDGE, 3 * numV / 10, 1, new boolean[]{true, true}, weight, Order.DESC);
        evaluateQuery(v.query().labels("connect").direction(OUT).has("time", Cmp.EQUAL, 22).orderBy("weight", decr),
                EDGE, 1, 1, new boolean[]{true, true}, weight, Order.DESC);
        evaluateQuery(v.query().labels("connect").direction(OUT).has("time", Cmp.EQUAL, 22).orderBy("weight", incr),
                EDGE, 1, 1, new boolean[]{true, false}, weight, Order.ASC);
        evaluateQuery(v.query().labels("connect").direction(OUT).adjacent(u),
                EDGE, 1, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("connect").direction(OUT).has("weight", Cmp.EQUAL, 0.0).adjacent(u),
                EDGE, 1, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("connect").direction(OUT).interval("weight", 0.0, 1.0).adjacent(u),
                EDGE, 1, 1, new boolean[]{false, true});
        evaluateQuery(v.query().labels("connect").direction(OUT).interval("time", 50, 100).adjacent(u),
                EDGE, 1, 1, new boolean[]{false, true});

        evaluateQuery(v.query(),
                RELATION, numV * 4, 1, new boolean[]{true, true});
        evaluateQuery(v.query().direction(OUT),
                RELATION, numV * 4, 1, new boolean[]{false, true});

        //--------------

        //Update in transaction
        for (Object o : v.query().labels("name").properties()) {
            JanusGraphVertexProperty<String> p = (JanusGraphVertexProperty<String>) o;
            if (p.<Long>value("time") < (numV / 2)) p.remove();
        }
        for (Object o : v.query().direction(BOTH).edges()) {
            JanusGraphEdge e = (JanusGraphEdge) o;
            if (e.<Long>value("time") < (numV / 2)) e.remove();
        }
        ns = new JanusGraphVertex[numV * 3 / 2];
        for (int i = numV; i < numV * 3 / 2; i++) {
            double w = (i * 0.5) % 5;
            long t = i;
            v.property("name", "v" + i, "weight", w, "time", t);

            ns[i] = tx.addVertex();
            for (String label : new String[]{"connect", "child", "link"}) {
                JanusGraphEdge e = v.addEdge(label, ns[i], "weight", w, "time", t);
            }
        }

        //######### UPDATED QUERIES ##########

        evaluateQuery(v.query().keys("name").has("weight", Cmp.GREATER_THAN, 3.6),
                PROPERTY, 2 * numV / 10, 1, new boolean[]{false, true});
        evaluateQuery(v.query().keys("name").interval("time", numV / 2 - 10, numV / 2 + 10),
                PROPERTY, 10, 1, new boolean[]{false, true});
        evaluateQuery(v.query().keys("name").interval("time", numV / 2 - 10, numV / 2 + 10).orderBy("weight", decr),
                PROPERTY, 10, 1, new boolean[]{false, false}, weight, Order.DESC);
        evaluateQuery(v.query().keys("name").interval("time", numV, numV + 10).limit(5),
                PROPERTY, 5, 1, new boolean[]{false, true});

        evaluateQuery(v.query().labels("child").direction(OUT).has("time", Cmp.EQUAL, 5),
                EDGE, 0, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("child").direction(OUT).has("time", Cmp.EQUAL, numV + 5),
                EDGE, 1, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("child").direction(OUT).interval("time", 10, 20).orderBy("weight", decr).limit(5),
                EDGE, 0, 1, new boolean[]{true, false}, weight, Order.DESC);
        evaluateQuery(v.query().labels("child").direction(OUT).interval("time", numV + 10, numV + 20).orderBy("weight", decr).limit(5),
                EDGE, 5, 1, new boolean[]{true, false}, weight, Order.DESC);


        evaluateQuery(v.query(),
                RELATION, numV * 4, 1, new boolean[]{true, true});
        evaluateQuery(v.query().direction(OUT),
                RELATION, numV * 4, 1, new boolean[]{false, true});

        //######### END UPDATED QUERIES ##########

        newTx();

        weight = tx.getPropertyKey("weight");
        time = tx.getPropertyKey("time");

        v = getV(tx, v);
        u = getV(tx, u);

        //######### UPDATED QUERIES (copied from above) ##########

        evaluateQuery(v.query().keys("name").has("weight", Cmp.GREATER_THAN, 3.6),
                PROPERTY, 2 * numV / 10, 1, new boolean[]{false, true});
        evaluateQuery(v.query().keys("name").interval("time", numV / 2 - 10, numV / 2 + 10),
                PROPERTY, 10, 1, new boolean[]{false, true});
        evaluateQuery(v.query().keys("name").interval("time", numV / 2 - 10, numV / 2 + 10).orderBy("weight", decr),
                PROPERTY, 10, 1, new boolean[]{false, false}, weight, Order.DESC);
        evaluateQuery(v.query().keys("name").interval("time", numV, numV + 10).limit(5),
                PROPERTY, 5, 1, new boolean[]{false, true});

        evaluateQuery(v.query().labels("child").direction(OUT).has("time", Cmp.EQUAL, 5),
                EDGE, 0, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("child").direction(OUT).has("time", Cmp.EQUAL, numV + 5),
                EDGE, 1, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("child").direction(OUT).interval("time", 10, 20).orderBy("weight", decr).limit(5),
                EDGE, 0, 1, new boolean[]{true, false}, weight, Order.DESC);
        evaluateQuery(v.query().labels("child").direction(OUT).interval("time", numV + 10, numV + 20).orderBy("weight", decr).limit(5),
                EDGE, 5, 1, new boolean[]{true, false}, weight, Order.DESC);


        evaluateQuery(v.query(),
                RELATION, numV * 4, 1, new boolean[]{true, true});
        evaluateQuery(v.query().direction(OUT),
                RELATION, numV * 4, 1, new boolean[]{false, true});

        //######### END UPDATED QUERIES ##########

    }

    private boolean isSortedByID(VertexList vl) {
        for (int i = 1; i < vl.size(); i++) {
            if (vl.getID(i - 1) > vl.getID(i)) return false;
        }
        return true;
    }

    public static void evaluateQuery(JanusGraphVertexQuery query, RelationCategory resultType,
                                     int expectedResults, int numSubQueries, boolean[] subQuerySpecs) {
        evaluateQuery(query, resultType, expectedResults, numSubQueries, subQuerySpecs, ImmutableMap.<PropertyKey, Order>of());
    }

    public static void evaluateQuery(JanusGraphVertexQuery query, RelationCategory resultType,
                                     int expectedResults, int numSubQueries, boolean[] subQuerySpecs,
                                     PropertyKey orderKey, Order order) {
        evaluateQuery(query, resultType, expectedResults, numSubQueries, subQuerySpecs, ImmutableMap.of(orderKey, order));
    }


    public static void evaluateQuery(JanusGraphVertexQuery query, RelationCategory resultType,
                                     int expectedResults, int numSubQueries, boolean[] subQuerySpecs,
                                     Map<PropertyKey, Order> orderMap) {
        SimpleQueryProfiler profiler = new SimpleQueryProfiler();
        ((BasicVertexCentricQueryBuilder) query).profiler(profiler);

        Iterable<? extends JanusGraphElement> result;
        switch (resultType) {
            case PROPERTY:
                result = query.properties();
                break;
            case EDGE:
                result = query.edges();
                break;
            case RELATION:
                result = query.relations();
                break;
            default:
                throw new AssertionError();
        }
        OrderList orders = profiler.getAnnotation(QueryProfiler.ORDERS_ANNOTATION);

        //Check elements and that they are returned in the correct order
        int no = 0;
        JanusGraphElement previous = null;
        for (JanusGraphElement e : result) {
            assertNotNull(e);
            no++;
            if (previous != null && !orders.isEmpty()) {
                assertTrue(orders.compare(previous, e) <= 0);
            }
            previous = e;
        }
        assertEquals(expectedResults, no);

        //Check OrderList of query
        assertNotNull(orders);
        assertEquals(orderMap.size(), orders.size());
        for (int i = 0; i < orders.size(); i++) {
            assertEquals(orderMap.get(orders.getKey(i)), orders.getOrder(i));
        }
        for (PropertyKey key : orderMap.keySet()) assertTrue(orders.containsKey(key));

        //Check subqueries
        assertEquals(Integer.valueOf(1), profiler.getAnnotation(QueryProfiler.NUMVERTICES_ANNOTATION));
        int subQueryCounter = 0;
        for (SimpleQueryProfiler subProfiler : profiler) {
            assertNotNull(subProfiler);
            if (subProfiler.getGroupName().equals(QueryProfiler.OPTIMIZATION)) continue;
            if (subQuerySpecs.length == 2) { //0=>fitted, 1=>ordered
                assertEquals(subQuerySpecs[0], subProfiler.getAnnotation(QueryProfiler.FITTED_ANNOTATION));
                assertEquals(subQuerySpecs[1], subProfiler.getAnnotation(QueryProfiler.ORDERED_ANNOTATION));
            }
            //assertEquals(1,Iterables.size(subProfiler)); This only applies if a disk call is necessary
            subQueryCounter++;
        }
        assertEquals(numSubQueries, subQueryCounter);
    }

    @Test
    public void testEdgesExceedCacheSize() {
        // Add a vertex with as many edges as the tx-cache-size. (20000 by default)
        int numEdges = graph.getConfiguration().getTxVertexCacheSize();
        JanusGraphVertex parentVertex = graph.addVertex();
        for (int i = 0; i < numEdges; i++) {
            JanusGraphVertex childVertex = graph.addVertex();
            parentVertex.addEdge("friend", childVertex);
        }
        graph.tx().commit();
        assertCount(numEdges, parentVertex.query().direction(Direction.OUT).edges());

        // Remove an edge.
        ((Edge) parentVertex.query().direction(OUT).edges().iterator().next()).remove();

        // Check that getEdges returns one fewer.
        assertCount(numEdges - 1, parentVertex.query().direction(Direction.OUT).edges());

        // Run the same check one more time.
        // This fails! (Expected: 19999. Actual: 20000.)
        assertCount(numEdges - 1, parentVertex.query().direction(Direction.OUT).edges());
    }


    @Test
    public void testTinkerPopCardinality() {
        PropertyKey id = mgmt.makePropertyKey("id").cardinality(Cardinality.SINGLE).dataType(Integer.class).make();
        PropertyKey name = mgmt.makePropertyKey("name").cardinality(Cardinality.SINGLE).dataType(String.class).make();
        PropertyKey names = mgmt.makePropertyKey("names").cardinality(Cardinality.LIST).dataType(String.class).make();

        mgmt.buildIndex("byId", Vertex.class).addKey(id).buildCompositeIndex();

        finishSchema();
        GraphTraversalSource gts;
        Vertex v;

        v = graph.addVertex("id", 1);
        v.property(single, "name", "t1");
        graph.addVertex("id", 2, "names", "n1", "names", "n2");
        graph.tx().commit();

        gts = graph.traversal();
        v = gts.V().has("id", 1).next();
        v.property(single, "name", "t2");
        v = gts.V().has("id", 1).next();
        v.property(single, "name", "t3");
        assertCount(1, gts.V(v).properties("name"));
        assertCount(2, gts.V().has("id", 2).properties("names"));
        assertCount(2, gts.V().hasLabel("vertex"));
    }

    @Test
    public void testTinkerPopOptimizationStrategies() {
        PropertyKey id = mgmt.makePropertyKey("id").cardinality(Cardinality.SINGLE).dataType(Integer.class).make();
        PropertyKey weight = mgmt.makePropertyKey("weight").cardinality(Cardinality.SINGLE).dataType(Integer.class).make();

        mgmt.buildIndex("byId", Vertex.class).addKey(id).buildCompositeIndex();
        mgmt.buildIndex("byWeight", Vertex.class).addKey(weight).buildCompositeIndex();
        mgmt.buildIndex("byIdWeight", Vertex.class).addKey(id).addKey(weight).buildCompositeIndex();

        EdgeLabel knows = mgmt.makeEdgeLabel("knows").make();
        mgmt.buildEdgeIndex(knows, "byWeightDecr", Direction.OUT, decr, weight);
        mgmt.buildEdgeIndex(knows, "byWeightIncr", Direction.OUT, incr, weight);

        PropertyKey names = mgmt.makePropertyKey("names").cardinality(Cardinality.LIST).dataType(String.class).make();
        mgmt.buildPropertyIndex(names, "namesByWeight", decr, weight);

        finishSchema();


        int numV = 100;
        JanusGraphVertex[] vs = new JanusGraphVertex[numV];
        for (int i = 0; i < numV; i++) {
            vs[i] = graph.addVertex("id", i, "weight", i % 5);
        }
        int superV = 10;
        int sid = -1;
        JanusGraphVertex[] sv = new JanusGraphVertex[superV];
        for (int i = 0; i < superV; i++) {
            sv[i] = graph.addVertex("id", sid);
            for (int j = 0; j < numV; j++) {
                sv[i].addEdge("knows", vs[j], "weight", j % 5);
                sv[i].property(VertexProperty.Cardinality.list, "names", "n" + j, "weight", j % 5);
            }
        }

        Traversal t;
        TraversalMetrics metrics;
        GraphTraversalSource gts = graph.traversal();

        //Edge
        assertNumStep(numV / 5, 1, gts.V(sv[0]).outE("knows").has("weight", 1), JanusGraphVertexStep.class);
        assertNumStep(numV, 1, gts.V(sv[0]).outE("knows"), JanusGraphVertexStep.class);
        assertNumStep(numV, 1, gts.V(sv[0]).out("knows"), JanusGraphVertexStep.class);
        assertNumStep(10, 1, gts.V(sv[0]).local(__.outE("knows").limit(10)), JanusGraphVertexStep.class);
        assertNumStep(10, 1, gts.V(sv[0]).local(__.outE("knows").range(10, 20)), LocalStep.class);
        assertNumStep(numV, 2, gts.V(sv[0]).outE("knows").order().by("weight", decr), JanusGraphVertexStep.class, OrderGlobalStep.class);
        // Ensure the LocalStep is dropped because the Order can be folded in the JanusGraphVertexStep which in turn
        // will allow JanusGraphLocalQueryOptimizationStrategy to drop the LocalStep as the local ordering will be
        // provided by the single JanusGraphVertex step
        assertNumStep(10, 0, gts.V(sv[0]).local(__.outE("knows").order().by("weight", decr).limit(10)), LocalStep.class);
        assertNumStep(numV / 5, 2, gts.V(sv[0]).outE("knows").has("weight", 1).order().by("weight", incr), JanusGraphVertexStep.class, OrderGlobalStep.class);
        assertNumStep(10, 0, gts.V(sv[0]).local(__.outE("knows").has("weight", 1).order().by("weight", incr).limit(10)), LocalStep.class);
        // Note that for this test, the upper offset of the range will be folded into the JanusGraphVertexStep
        // by JanusGraphLocalQueryOptimizationStrategy, but not the lower offset. The RangeGlobalStep will in turn be kept
        // to enforce this lower bound and the LocalStep will be left as is as the local behavior will have not been
        // entirely subsumed by the JanusGraphVertexStep
        assertNumStep(5, 1, gts.V(sv[0]).local(__.outE("knows").has("weight", 1).has("weight", 1).order().by("weight", incr).range(10, 15)), LocalStep.class);
        assertNumStep(1, 1, gts.V(sv[0]).outE("knows").filter(__.inV().is(vs[50])), JanusGraphVertexStep.class);
        assertNumStep(1, 1, gts.V(sv[0]).outE("knows").filter(__.otherV().is(vs[50])), JanusGraphVertexStep.class);
        assertNumStep(1, 1, gts.V(sv[0]).bothE("knows").filter(__.otherV().is(vs[50])), JanusGraphVertexStep.class);
        assertNumStep(1, 2, gts.V(sv[0]).bothE("knows").filter(__.inV().is(vs[50])), JanusGraphVertexStep.class, TraversalFilterStep.class);

        //Property
        assertNumStep(numV / 5, 1, gts.V(sv[0]).properties("names").has("weight", 1), JanusGraphPropertiesStep.class);
        assertNumStep(numV, 1, gts.V(sv[0]).properties("names"), JanusGraphPropertiesStep.class);
        assertNumStep(10, 0, gts.V(sv[0]).local(__.properties("names").order().by("weight", decr).limit(10)), LocalStep.class);
        assertNumStep(numV, 2, gts.V(sv[0]).outE("knows").values("weight"), JanusGraphVertexStep.class, JanusGraphPropertiesStep.class);


        //Global graph queries
        assertNumStep(1, 1, gts.V().has("id", numV / 5), JanusGraphStep.class);
        assertNumStep(1, 1, gts.V().has("id", numV / 5).has("weight", (numV / 5) % 5), JanusGraphStep.class);
        assertNumStep(numV / 5, 1, gts.V().has("weight", 1), JanusGraphStep.class);
        assertNumStep(10, 1, gts.V().has("weight", 1).range(0, 10), JanusGraphStep.class);

        assertNumStep(superV, 1, gts.V().has("id", sid), JanusGraphStep.class);
        //Ensure that as steps don't interfere
        assertNumStep(1, 1, gts.V().has("id", numV / 5).as("x"), JanusGraphStep.class);
        assertNumStep(1, 1, gts.V().has("id", numV / 5).has("weight", (numV / 5) % 5).as("x"), JanusGraphStep.class);


        assertNumStep(superV * (numV / 5), 2, gts.V().has("id", sid).outE("knows").has("weight", 1), JanusGraphStep.class, JanusGraphVertexStep.class);
        assertNumStep(superV * (numV / 5 * 2), 2, gts.V().has("id", sid).outE("knows").has("weight", P.gte(1)).has("weight", P.lt(3)), JanusGraphStep.class, JanusGraphVertexStep.class);
        assertNumStep(superV * (numV / 5 * 2), 2, gts.V().has("id", sid).outE("knows").has("weight", P.between(1, 3)), JanusGraphStep.class, JanusGraphVertexStep.class);
        assertNumStep(superV * 10, 2, gts.V().has("id", sid).local(__.outE("knows").has("weight", P.gte(1)).has("weight", P.lt(3)).limit(10)), JanusGraphStep.class, JanusGraphVertexStep.class);
        assertNumStep(superV * 10, 1, gts.V().has("id", sid).local(__.outE("knows").has("weight", P.between(1, 3)).order().by("weight", decr).limit(10)), JanusGraphStep.class);
        assertNumStep(superV * 10, 0, gts.V().has("id", sid).local(__.outE("knows").has("weight", P.between(1, 3)).order().by("weight", decr).limit(10)), LocalStep.class);
        clopen(option(USE_MULTIQUERY), true);
        gts = graph.traversal();

        assertNumStep(superV * (numV / 5), 2, gts.V().has("id", sid).outE("knows").has("weight", 1), JanusGraphStep.class, JanusGraphVertexStep.class);
        assertNumStep(superV * (numV / 5 * 2), 2, gts.V().has("id", sid).outE("knows").has("weight", P.between(1, 3)), JanusGraphStep.class, JanusGraphVertexStep.class);
        assertNumStep(superV * 10, 2, gts.V().has("id", sid).local(__.outE("knows").has("weight", P.gte(1)).has("weight", P.lt(3)).limit(10)), JanusGraphStep.class, JanusGraphVertexStep.class);
        assertNumStep(superV * 10, 1, gts.V().has("id", sid).local(__.outE("knows").has("weight", P.between(1, 3)).order().by("weight", decr).limit(10)), JanusGraphStep.class);
        assertNumStep(superV * 10, 0, gts.V().has("id", sid).local(__.outE("knows").has("weight", P.between(1, 3)).order().by("weight", decr).limit(10)), LocalStep.class);
        assertNumStep(superV * numV, 2, gts.V().has("id", sid).values("names"), JanusGraphStep.class, JanusGraphPropertiesStep.class);

        //Verify traversal metrics when all reads are from cache (i.e. no backend queries)
        t = gts.V().has("id", sid).local(__.outE("knows").has("weight", P.between(1, 3)).order().by("weight", decr).limit(10)).profile("~metrics");
        assertCount(superV * 10, t);
        metrics = (TraversalMetrics) t.asAdmin().getSideEffects().get("~metrics");

        //Verify that properties also use multi query
        t = gts.V().has("id", sid).values("names").profile("~metrics");
        assertCount(superV * numV, t);
        metrics = (TraversalMetrics) t.asAdmin().getSideEffects().get("~metrics");

        clopen(option(USE_MULTIQUERY), true);
        gts = graph.traversal();

        //Verify traversal metrics when having to read from backend [same query as above]
        t = gts.V().has("id", sid).local(__.outE("knows").has("weight", P.gte(1)).has("weight", P.lt(3)).order().by("weight", decr).limit(10)).profile("~metrics");
        assertCount(superV * 10, t); 
        metrics = (TraversalMetrics) t.asAdmin().getSideEffects().get("~metrics");

        //Verify that properties also use multi query [same query as above]
        t = gts.V().has("id", sid).values("names").profile("~metrics");
        assertCount(superV * numV, t);
        metrics = (TraversalMetrics) t.asAdmin().getSideEffects().get("~metrics");

    }

    private static void assertNumStep(int expectedResults, int expectedSteps, GraphTraversal traversal, Class<? extends Step>... expectedStepTypes) {
        int num = 0;
        while (traversal.hasNext()) {
            traversal.next();
            num++;
        }

        assertEquals(expectedResults, num);

        //Verify that steps line up with what is expected after JanusGraph's optimizations are applied
        List<Step> steps = traversal.asAdmin().getSteps();
        Set<Class<? extends Step>> expSteps = Sets.newHashSet(expectedStepTypes);
        int numSteps = 0;
        for (Step s : steps) {
            if (s.getClass().equals(GraphStep.class) || s.getClass().equals(StartStep.class)) continue;

            if (expSteps.contains(s.getClass())) {
                numSteps++;
            }
        }
        assertEquals(expectedSteps, numSteps);
    }

    private static void verifyMetrics(Metrics metric, boolean fromCache, boolean multiQuery) {
        assertTrue(metric.getDuration(TimeUnit.MICROSECONDS) > 0);
        assertTrue(metric.getCount(TraversalMetrics.ELEMENT_COUNT_ID) > 0);
        String hasMultiQuery = (String) metric.getAnnotation(QueryProfiler.MULTIQUERY_ANNOTATION);
        assertTrue(multiQuery ? hasMultiQuery.equalsIgnoreCase("true") : hasMultiQuery == null);
        for (Metrics submetric : metric.getNested()) {
            assertTrue(submetric.getDuration(TimeUnit.MICROSECONDS) > 0);
            switch (submetric.getName()) {
                case "optimization":
                    assertNull(submetric.getCount(TraversalMetrics.ELEMENT_COUNT_ID));
                    break;
                case "backend-query":
                    if (fromCache) assertFalse("Should not execute backend-queries when cached", true);
                    assertTrue(submetric.getCount(TraversalMetrics.ELEMENT_COUNT_ID) > 0);
                    break;
                default:
                    assertFalse("Unrecognized nested query: " + submetric.getName(), true);
            }

        }

    }


    @Test
    public void testSimpleTinkerPopTraversal() {
        Vertex v1 = graph.addVertex("name", "josh");
        Vertex v2 = graph.addVertex("name", "lop");
        v1.addEdge("created", v2);
        //graph.tx().commit();

        Object id = graph.traversal().V().has("name", "josh").outE("created").as("e").inV().has("name", "lop").<Edge>select("e").next().id();
        assertNotNull(id);
    }


   /* ==================================================================================
                            LOGGING
     ==================================================================================*/


    @Test
    public void simpleLogTest() throws InterruptedException {
        simpleLogTest(false);
    }

    @Test
    public void simpleLogTestWithFailure() throws InterruptedException {
        simpleLogTest(true);
    }

    public void simpleLogTest(final boolean withLogFailure) throws InterruptedException {
        final String userlogName = "test";
        final Serializer serializer = graph.getDataSerializer();
        final EdgeSerializer edgeSerializer = graph.getEdgeSerializer();
        final TimestampProvider times = graph.getConfiguration().getTimestampProvider();
        final Instant startTime = times.getTime();
        clopen(option(SYSTEM_LOG_TRANSACTIONS), true,
                option(LOG_BACKEND, USER_LOG), (withLogFailure ? TestMockLog.class.getName() : LOG_BACKEND.getDefaultValue()),
                option(TestMockLog.LOG_MOCK_FAILADD, USER_LOG), withLogFailure,
                option(KCVSLog.LOG_READ_LAG_TIME, USER_LOG), Duration.ofMillis(50),
                option(LOG_READ_INTERVAL, USER_LOG), Duration.ofMillis(250),
                option(LOG_SEND_DELAY, USER_LOG), Duration.ofMillis(100),
                option(KCVSLog.LOG_READ_LAG_TIME, TRANSACTION_LOG), Duration.ofMillis(50),
                option(LOG_READ_INTERVAL, TRANSACTION_LOG), Duration.ofMillis(250),
                option(MAX_COMMIT_TIME), Duration.ofSeconds(1)
        );
        final String instanceid = graph.getConfiguration().getUniqueGraphId();

        PropertyKey weight = tx.makePropertyKey("weight").dataType(Float.class).cardinality(Cardinality.SINGLE).make();
        EdgeLabel knows = tx.makeEdgeLabel("knows").make();
        JanusGraphVertex n1 = tx.addVertex("weight", 10.5);
        newTx();

        final Instant txTimes[] = new Instant[4];
        //Transaction with custom userlog name
        txTimes[0] = times.getTime();
        JanusGraphTransaction tx2 = graph.buildTransaction().logIdentifier(userlogName).start();
        JanusGraphVertex v1 = tx2.addVertex("weight", 111.1);
        v1.addEdge("knows", v1);
        tx2.commit();
        final long v1id = getId(v1);
        txTimes[1] = times.getTime();
        tx2 = graph.buildTransaction().logIdentifier(userlogName).start();
        JanusGraphVertex v2 = tx2.addVertex("weight", 222.2);
        v2.addEdge("knows", getV(tx2, v1id));
        tx2.commit();
        final long v2id = getId(v2);
        //Only read tx
        tx2 = graph.buildTransaction().logIdentifier(userlogName).start();
        v1 = getV(tx2, v1id);
        assertEquals(111.1, v1.<Float>value("weight").doubleValue(), 0.01);
        assertEquals(222.2, getV(tx2, v2).<Float>value("weight").doubleValue(), 0.01);
        tx2.commit();
        //Deleting transaction
        txTimes[2] = times.getTime();
        tx2 = graph.buildTransaction().logIdentifier(userlogName).start();
        v2 = getV(tx2, v2id);
        assertEquals(222.2, v2.<Float>value("weight").doubleValue(), 0.01);
        v2.remove();
        tx2.commit();
        //Edge modifying transaction
        txTimes[3] = times.getTime();
        tx2 = graph.buildTransaction().logIdentifier(userlogName).start();
        v1 = getV(tx2, v1id);
        assertEquals(111.1, v1.<Float>value("weight").doubleValue(), 0.01);
        Edge e = Iterables.<JanusGraphEdge>getOnlyElement(v1.query().direction(Direction.OUT).labels("knows").edges());
        assertFalse(e.property("weight").isPresent());
        e.property("weight", 44.4);
        tx2.commit();
        close();
        final Instant endTime = times.getTime();

        final ReadMarker startMarker = ReadMarker.fromTime(startTime);

        Log txlog = openTxLog();
        Log userLog = openUserLog(userlogName);
        final EnumMap<LogTxStatus, AtomicInteger> txMsgCounter = new EnumMap<LogTxStatus, AtomicInteger>(LogTxStatus.class);
        for (LogTxStatus status : LogTxStatus.values()) txMsgCounter.put(status, new AtomicInteger(0));
        final AtomicInteger userlogMeta = new AtomicInteger(0);
        txlog.registerReader(startMarker, new MessageReader() {
            @Override public void updateState() {}

            @Override
            public void read(Message message) {
                Instant msgTime = message.getTimestamp();
                assertTrue(msgTime.isAfter(startTime) || msgTime.equals(startTime));
                assertNotNull(message.getSenderId());
                TransactionLogHeader.Entry txEntry = TransactionLogHeader.parse(message.getContent(), serializer, times);
                TransactionLogHeader header = txEntry.getHeader();
//                System.out.println(header.getTimestamp(TimeUnit.MILLISECONDS));
                assertTrue(header.getTimestamp().isAfter(startTime) || header.getTimestamp().equals(startTime));
                assertTrue(header.getTimestamp().isBefore(msgTime) || header.getTimestamp().equals(msgTime));
                assertNotNull(txEntry.getMetadata());
                assertNull(txEntry.getMetadata().get(LogTxMeta.GROUPNAME));
                LogTxStatus status = txEntry.getStatus();
                if (status == LogTxStatus.PRECOMMIT) {
                    assertTrue(txEntry.hasContent());
                    Object logid = txEntry.getMetadata().get(LogTxMeta.LOG_ID);
                    if (logid != null) {
                        assertTrue(logid instanceof String);
                        assertEquals(userlogName, logid);
                        userlogMeta.incrementAndGet();
                    }
                } else if (withLogFailure) {
                    assertTrue(status.isPrimarySuccess() || status == LogTxStatus.SECONDARY_FAILURE);
                    if (status == LogTxStatus.SECONDARY_FAILURE) {
                        TransactionLogHeader.SecondaryFailures secFail = txEntry.getContentAsSecondaryFailures(serializer);
                        assertTrue(secFail.failedIndexes.isEmpty());
                        assertTrue(secFail.userLogFailure);
                    }
                } else {
                    assertFalse(txEntry.hasContent());
                    assertTrue(status.isSuccess());
                }
                txMsgCounter.get(txEntry.getStatus()).incrementAndGet();
            }
        });
        final EnumMap<Change, AtomicInteger> userChangeCounter = new EnumMap<Change, AtomicInteger>(Change.class);
        for (Change change : Change.values()) userChangeCounter.put(change, new AtomicInteger(0));
        final AtomicInteger userLogMsgCounter = new AtomicInteger(0);
        userLog.registerReader(startMarker, new MessageReader() {
            @Override
            public void updateState() {}

            @Override
            public void read(Message message) {
                Instant msgTime = message.getTimestamp();
                assertTrue(msgTime.isAfter(startTime) || msgTime.equals(startTime));
                assertNotNull(message.getSenderId());
                StaticBuffer content = message.getContent();
                assertTrue(content != null && content.length() > 0);
                TransactionLogHeader.Entry txentry = TransactionLogHeader.parse(content, serializer, times);

                Instant txTime = txentry.getHeader().getTimestamp();
                assertTrue(txTime.isBefore(msgTime) || txTime.equals(msgTime));
                assertTrue(txTime.isAfter(startTime) || txTime.equals(msgTime));
                long txid = txentry.getHeader().getId();
                assertTrue(txid > 0);
                for (TransactionLogHeader.Modification modification : txentry.getContentAsModifications(serializer)) {
                    assertTrue(modification.state == Change.ADDED || modification.state == Change.REMOVED);
                    userChangeCounter.get(modification.state).incrementAndGet();
                }
                userLogMsgCounter.incrementAndGet();
            }
        });
        Thread.sleep(4000);
        assertEquals(5, txMsgCounter.get(LogTxStatus.PRECOMMIT).get());
        assertEquals(4, txMsgCounter.get(LogTxStatus.PRIMARY_SUCCESS).get());
        assertEquals(1, txMsgCounter.get(LogTxStatus.COMPLETE_SUCCESS).get());
        assertEquals(4, userlogMeta.get());
        if (withLogFailure) assertEquals(4, txMsgCounter.get(LogTxStatus.SECONDARY_FAILURE).get());
        else assertEquals(4, txMsgCounter.get(LogTxStatus.SECONDARY_SUCCESS).get());
        //User-Log
        if (withLogFailure) {
            assertEquals(0, userLogMsgCounter.get());
        } else {
            assertEquals(4, userLogMsgCounter.get());
            assertEquals(7, userChangeCounter.get(Change.ADDED).get());
            assertEquals(4, userChangeCounter.get(Change.REMOVED).get());
        }

        clopen(option(VERBOSE_TX_RECOVERY), true);
        /*
        Transaction Recovery
         */
        TransactionRecovery recovery = JanusGraphFactory.startTransactionRecovery(graph, startTime);


        /*
        Use user log processing framework
         */
        final AtomicInteger userLogCount = new AtomicInteger(0);
        LogProcessorFramework userlogs = JanusGraphFactory.openTransactionLog(graph);
        userlogs.addLogProcessor(userlogName).setStartTime(startTime).setRetryAttempts(1)
                .addProcessor(new ChangeProcessor() {
                    @Override
                    public void process(JanusGraphTransaction tx, TransactionId txId, ChangeState changes) {
                        assertEquals(instanceid, txId.getInstanceId());
                        assertTrue(txId.getTransactionId() > 0 && txId.getTransactionId() < 100); //Just some reasonable upper bound
                        final Instant txTime = txId.getTransactionTime();
                        assertTrue(String.format("tx timestamp %s not between start %s and end time %s",
                                        txTime, startTime, endTime),
                                (txTime.isAfter(startTime) || txTime.equals(startTime)) && (txTime.isBefore(endTime) || txTime.equals(endTime))); //Times should be within a second

                        assertTrue(tx.containsRelationType("knows"));
                        assertTrue(tx.containsRelationType("weight"));
                        EdgeLabel knows = tx.getEdgeLabel("knows");
                        PropertyKey weight = tx.getPropertyKey("weight");

                        Instant txTimeMicro = txId.getTransactionTime();

                        int txNo;
                        if (txTimeMicro.isBefore(txTimes[1])) {
                            txNo = 1;
                            //v1 addition transaction
                            assertEquals(1, Iterables.size(changes.getVertices(Change.ADDED)));
                            assertEquals(0, Iterables.size(changes.getVertices(Change.REMOVED)));
                            assertEquals(1, Iterables.size(changes.getVertices(Change.ANY)));
                            assertEquals(2, Iterables.size(changes.getRelations(Change.ADDED)));
                            assertEquals(1, Iterables.size(changes.getRelations(Change.ADDED, knows)));
                            assertEquals(1, Iterables.size(changes.getRelations(Change.ADDED, weight)));
                            assertEquals(2, Iterables.size(changes.getRelations(Change.ANY)));
                            assertEquals(0, Iterables.size(changes.getRelations(Change.REMOVED)));

                            JanusGraphVertex v = Iterables.getOnlyElement(changes.getVertices(Change.ADDED));
                            assertEquals(v1id, getId(v));
                            VertexProperty<Float> p = Iterables.getOnlyElement(changes.getProperties(v, Change.ADDED, "weight"));
                            assertEquals(111.1, p.value().doubleValue(), 0.01);
                            assertEquals(1, Iterables.size(changes.getEdges(v, Change.ADDED, OUT)));
                            assertEquals(1, Iterables.size(changes.getEdges(v, Change.ADDED, BOTH)));
                        } else if (txTimeMicro.isBefore(txTimes[2])) {
                            txNo = 2;
                            //v2 addition transaction
                            assertEquals(1, Iterables.size(changes.getVertices(Change.ADDED)));
                            assertEquals(0, Iterables.size(changes.getVertices(Change.REMOVED)));
                            assertEquals(2, Iterables.size(changes.getVertices(Change.ANY)));
                            assertEquals(2, Iterables.size(changes.getRelations(Change.ADDED)));
                            assertEquals(1, Iterables.size(changes.getRelations(Change.ADDED, knows)));
                            assertEquals(1, Iterables.size(changes.getRelations(Change.ADDED, weight)));
                            assertEquals(2, Iterables.size(changes.getRelations(Change.ANY)));
                            assertEquals(0, Iterables.size(changes.getRelations(Change.REMOVED)));

                            JanusGraphVertex v = Iterables.getOnlyElement(changes.getVertices(Change.ADDED));
                            assertEquals(v2id, getId(v));
                            VertexProperty<Float> p = Iterables.getOnlyElement(changes.getProperties(v, Change.ADDED, "weight"));
                            assertEquals(222.2, p.value().doubleValue(), 0.01);
                            assertEquals(1, Iterables.size(changes.getEdges(v, Change.ADDED, OUT)));
                            assertEquals(1, Iterables.size(changes.getEdges(v, Change.ADDED, BOTH)));
                        } else if (txTimeMicro.isBefore(txTimes[3])) {
                            txNo = 3;
                            //v2 deletion transaction
                            assertEquals(0, Iterables.size(changes.getVertices(Change.ADDED)));
                            assertEquals(1, Iterables.size(changes.getVertices(Change.REMOVED)));
                            assertEquals(2, Iterables.size(changes.getVertices(Change.ANY)));
                            assertEquals(0, Iterables.size(changes.getRelations(Change.ADDED)));
                            assertEquals(2, Iterables.size(changes.getRelations(Change.REMOVED)));
                            assertEquals(1, Iterables.size(changes.getRelations(Change.REMOVED, knows)));
                            assertEquals(1, Iterables.size(changes.getRelations(Change.REMOVED, weight)));
                            assertEquals(2, Iterables.size(changes.getRelations(Change.ANY)));

                            JanusGraphVertex v = Iterables.getOnlyElement(changes.getVertices(Change.REMOVED));
                            assertEquals(v2id, getId(v));
                            VertexProperty<Float> p = Iterables.getOnlyElement(changes.getProperties(v, Change.REMOVED, "weight"));
                            assertEquals(222.2, p.value().doubleValue(), 0.01);
                            assertEquals(1, Iterables.size(changes.getEdges(v, Change.REMOVED, OUT)));
                            assertEquals(0, Iterables.size(changes.getEdges(v, Change.ADDED, BOTH)));
                        } else {
                            txNo = 4;
                            //v1 edge modification
                            assertEquals(0, Iterables.size(changes.getVertices(Change.ADDED)));
                            assertEquals(0, Iterables.size(changes.getVertices(Change.REMOVED)));
                            assertEquals(1, Iterables.size(changes.getVertices(Change.ANY)));
                            assertEquals(1, Iterables.size(changes.getRelations(Change.ADDED)));
                            assertEquals(1, Iterables.size(changes.getRelations(Change.REMOVED)));
                            assertEquals(1, Iterables.size(changes.getRelations(Change.REMOVED, knows)));
                            assertEquals(2, Iterables.size(changes.getRelations(Change.ANY)));

                            JanusGraphVertex v = Iterables.getOnlyElement(changes.getVertices(Change.ANY));
                            assertEquals(v1id, getId(v));
                            JanusGraphEdge e = Iterables.getOnlyElement(changes.getEdges(v, Change.REMOVED, Direction.OUT, "knows"));
                            assertFalse(e.property("weight").isPresent());
                            assertEquals(v, e.vertex(Direction.IN));
                            e = Iterables.getOnlyElement(changes.getEdges(v, Change.ADDED, Direction.OUT, "knows"));
                            assertEquals(44.4, e.<Float>value("weight").doubleValue(), 0.01);
                            assertEquals(v, e.vertex(Direction.IN));
                        }

                        //See only current state of graph in transaction
                        JanusGraphVertex v1 = getV(tx, v1id);
                        assertNotNull(v1);
                        assertTrue(v1.isLoaded());
                        if (txNo != 2) {
                            //In the transaction that adds v2, v2 will be considered "loaded"
                            assertMissing(tx, v2id);
//                    assertTrue(txNo + " - " + v2, v2 == null || v2.isRemoved());
                        }
                        assertEquals(111.1, v1.<Float>value("weight").doubleValue(), 0.01);
                        assertCount(1, v1.query().direction(Direction.OUT).edges());

                        userLogCount.incrementAndGet();
                    }
                }).build();

        //wait
        Thread.sleep(22000L);

        recovery.shutdown();
        long[] recoveryStats = ((StandardTransactionLogProcessor) recovery).getStatistics();
        if (withLogFailure) {
            assertEquals(1, recoveryStats[0]);
            assertEquals(4, recoveryStats[1]);
        } else {
            assertEquals(5, recoveryStats[0]);
            assertEquals(0, recoveryStats[1]);

        }

        userlogs.removeLogProcessor(userlogName);
        userlogs.shutdown();
        assertEquals(4, userLogCount.get());
    }


   /* ==================================================================================
                            GLOBAL GRAPH QUERIES
     ==================================================================================*/


    /**
     * Tests index defintions and their correct application for internal indexes only
     */
    @Test
    public void testGlobalGraphIndexingAndQueriesForInternalIndexes() {
        PropertyKey weight = makeKey("weight", Float.class);
        PropertyKey time = makeKey("time", Long.class);
        PropertyKey text = makeKey("text", String.class);

        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.LIST).make();
        EdgeLabel connect = mgmt.makeEdgeLabel("connect").signature(weight).make();
        EdgeLabel related = mgmt.makeEdgeLabel("related").signature(time).make();

        VertexLabel person = mgmt.makeVertexLabel("person").make();
        VertexLabel organization = mgmt.makeVertexLabel("organization").make();

        JanusGraphIndex edge1 = mgmt.buildIndex("edge1", Edge.class).addKey(time).addKey(weight).buildCompositeIndex();
        JanusGraphIndex edge2 = mgmt.buildIndex("edge2", Edge.class).indexOnly(connect).addKey(text).buildCompositeIndex();

        JanusGraphIndex prop1 = mgmt.buildIndex("prop1", JanusGraphVertexProperty.class).addKey(time).buildCompositeIndex();
        JanusGraphIndex prop2 = mgmt.buildIndex("prop2", JanusGraphVertexProperty.class).addKey(weight).addKey(text).buildCompositeIndex();

        JanusGraphIndex vertex1 = mgmt.buildIndex("vertex1", Vertex.class).addKey(time).indexOnly(person).unique().buildCompositeIndex();
        JanusGraphIndex vertex12 = mgmt.buildIndex("vertex12", Vertex.class).addKey(text).indexOnly(person).buildCompositeIndex();
        JanusGraphIndex vertex2 = mgmt.buildIndex("vertex2", Vertex.class).addKey(time).addKey(name).indexOnly(organization).buildCompositeIndex();
        JanusGraphIndex vertex3 = mgmt.buildIndex("vertex3", Vertex.class).addKey(name).buildCompositeIndex();


        // ########### INSPECTION & FAILURE ##############
        assertTrue(mgmt.containsRelationType("name"));
        assertTrue(mgmt.containsGraphIndex("prop1"));
        assertFalse(mgmt.containsGraphIndex("prop3"));
        assertEquals(2, Iterables.size(mgmt.getGraphIndexes(Edge.class)));
        assertEquals(2, Iterables.size(mgmt.getGraphIndexes(JanusGraphVertexProperty.class)));
        assertEquals(4, Iterables.size(mgmt.getGraphIndexes(Vertex.class)));
        assertNull(mgmt.getGraphIndex("balblub"));

        edge1 = mgmt.getGraphIndex("edge1");
        edge2 = mgmt.getGraphIndex("edge2");
        prop1 = mgmt.getGraphIndex("prop1");
        prop2 = mgmt.getGraphIndex("prop2");
        vertex1 = mgmt.getGraphIndex("vertex1");
        vertex12 = mgmt.getGraphIndex("vertex12");
        vertex2 = mgmt.getGraphIndex("vertex2");
        vertex3 = mgmt.getGraphIndex("vertex3");

        assertTrue(vertex1.isUnique());
        assertFalse(edge2.isUnique());
        assertEquals("prop1", prop1.name());
        assertTrue(Vertex.class.isAssignableFrom(vertex3.getIndexedElement()));
        assertTrue(JanusGraphVertexProperty.class.isAssignableFrom(prop1.getIndexedElement()));
        assertTrue(Edge.class.isAssignableFrom(edge2.getIndexedElement()));
        assertEquals(2, vertex2.getFieldKeys().length);
        assertEquals(1, vertex1.getFieldKeys().length);

        try {
            //Parameters not supported
            mgmt.buildIndex("blablub", Vertex.class).addKey(text, Mapping.TEXT.asParameter()).buildCompositeIndex();
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //Name already in use
            mgmt.buildIndex("edge1", Vertex.class).addKey(weight).buildCompositeIndex();
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //ImplicitKeys not allowed
            mgmt.buildIndex("jupdup", Vertex.class).addKey(ImplicitKey.ID).buildCompositeIndex();
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //Unique is only allowed for vertex
            mgmt.buildIndex("edgexyz", Edge.class).addKey(time).unique().buildCompositeIndex();
            fail();
        } catch (IllegalArgumentException e) {
        }

        // ########### END INSPECTION & FAILURE ##############
        finishSchema();
        clopen();

        text = mgmt.getPropertyKey("text");
        time = mgmt.getPropertyKey("time");
        weight = mgmt.getPropertyKey("weight");

        // ########### INSPECTION & FAILURE (copied from above) ##############
        assertTrue(mgmt.containsRelationType("name"));
        assertTrue(mgmt.containsGraphIndex("prop1"));
        assertFalse(mgmt.containsGraphIndex("prop3"));
        assertEquals(2, Iterables.size(mgmt.getGraphIndexes(Edge.class)));
        assertEquals(2, Iterables.size(mgmt.getGraphIndexes(JanusGraphVertexProperty.class)));
        assertEquals(4, Iterables.size(mgmt.getGraphIndexes(Vertex.class)));
        assertNull(mgmt.getGraphIndex("balblub"));

        edge1 = mgmt.getGraphIndex("edge1");
        edge2 = mgmt.getGraphIndex("edge2");
        prop1 = mgmt.getGraphIndex("prop1");
        prop2 = mgmt.getGraphIndex("prop2");
        vertex1 = mgmt.getGraphIndex("vertex1");
        vertex12 = mgmt.getGraphIndex("vertex12");
        vertex2 = mgmt.getGraphIndex("vertex2");
        vertex3 = mgmt.getGraphIndex("vertex3");

        assertTrue(vertex1.isUnique());
        assertFalse(edge2.isUnique());
        assertEquals("prop1", prop1.name());
        assertTrue(Vertex.class.isAssignableFrom(vertex3.getIndexedElement()));
        assertTrue(JanusGraphVertexProperty.class.isAssignableFrom(prop1.getIndexedElement()));
        assertTrue(Edge.class.isAssignableFrom(edge2.getIndexedElement()));
        assertEquals(2, vertex2.getFieldKeys().length);
        assertEquals(1, vertex1.getFieldKeys().length);

        try {
            //Parameters not supported
            mgmt.buildIndex("blablub", Vertex.class).addKey(text, Mapping.TEXT.asParameter()).buildCompositeIndex();
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //Name already in use
            mgmt.buildIndex("edge1", Vertex.class).addKey(weight).buildCompositeIndex();
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //ImplicitKeys not allowed
            mgmt.buildIndex("jupdup", Vertex.class).addKey(ImplicitKey.ID).buildCompositeIndex();
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            //Unique is only allowed for vertex
            mgmt.buildIndex("edgexyz", Edge.class).addKey(time).unique().buildCompositeIndex();
            fail();
        } catch (IllegalArgumentException e) {
        }

        // ########### END INSPECTION & FAILURE ##############

        final int numV = 100;
        final boolean sorted = true;
        JanusGraphVertex ns[] = new JanusGraphVertex[numV];
        String[] strs = {"aaa", "bbb", "ccc", "ddd"};

        for (int i = 0; i < numV; i++) {
            ns[i] = tx.addVertex(i % 2 == 0 ? "person" : "organization");
            VertexProperty p1 = ns[i].property("name", "v" + i);
            VertexProperty p2 = ns[i].property("name", "u" + (i % 5));

            double w = (i * 0.5) % 5;
            long t = i;
            String txt = strs[i % (strs.length)];

            ns[i].property(VertexProperty.Cardinality.single, "weight", w);
            ns[i].property(VertexProperty.Cardinality.single, "time", t);
            ns[i].property(VertexProperty.Cardinality.single, "text", txt);

            for (VertexProperty p : new VertexProperty[]{p1, p2}) {
                p.property("weight", w);
                p.property("time", t);
                p.property("text", txt);
            }

            JanusGraphVertex u = ns[(i > 0 ? i - 1 : i)]; //previous or self-loop
            for (String label : new String[]{"connect", "related"}) {
                Edge e = ns[i].addEdge(label, u, "weight", (w++) % 5, "time", t, "text", txt);
            }
        }

        //########## QUERIES ################
        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 10).has("weight", Cmp.EQUAL, 0),
                ElementCategory.EDGE, 1, new boolean[]{true, sorted}, edge1.name());
        evaluateQuery(tx.query().has("time", Contain.IN, ImmutableList.of(10, 20, 30)).has("weight", Cmp.EQUAL, 0),
                ElementCategory.EDGE, 3, new boolean[]{true, sorted}, edge1.name());
        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 10).has("weight", Cmp.EQUAL, 0).has("text", Cmp.EQUAL, strs[10 % strs.length]),
                ElementCategory.EDGE, 1, new boolean[]{false, sorted}, edge1.name());
        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 10).has("weight", Cmp.EQUAL, 1),
                ElementCategory.EDGE, 1, new boolean[]{true, sorted}, edge1.name());
        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 20).has("weight", Cmp.EQUAL, 0),
                ElementCategory.EDGE, 1, new boolean[]{true, sorted}, edge1.name());
        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 20).has("weight", Cmp.EQUAL, 3),
                ElementCategory.EDGE, 0, new boolean[]{true, sorted}, edge1.name());
        evaluateQuery(tx.query().has("text", Cmp.EQUAL, strs[0]).has(LABEL_NAME, "connect"),
                ElementCategory.EDGE, numV / strs.length, new boolean[]{true, sorted}, edge2.name());
        evaluateQuery(tx.query().has("text", Cmp.EQUAL, strs[0]).has(LABEL_NAME, "connect").limit(10),
                ElementCategory.EDGE, 10, new boolean[]{true, sorted}, edge2.name());
        evaluateQuery(tx.query().has("text", Cmp.EQUAL, strs[0]),
                ElementCategory.EDGE, numV / strs.length * 2, new boolean[]{false, sorted});
        evaluateQuery(tx.query().has("weight", Cmp.EQUAL, 1.5),
                ElementCategory.EDGE, numV / 10 * 2, new boolean[]{false, sorted});

        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 50),
                ElementCategory.PROPERTY, 2, new boolean[]{true, sorted}, prop1.name());
        evaluateQuery(tx.query().has("weight", Cmp.EQUAL, 0.0).has("text", Cmp.EQUAL, strs[0]),
                ElementCategory.PROPERTY, 2 * numV / (4 * 5), new boolean[]{true, sorted}, prop2.name());
        evaluateQuery(tx.query().has("weight", Cmp.EQUAL, 0.0).has("text", Cmp.EQUAL, strs[0]).has("time", Cmp.EQUAL, 0),
                ElementCategory.PROPERTY, 2, new boolean[]{true, sorted}, prop2.name(), prop1.name());
        evaluateQuery(tx.query().has("weight", Cmp.EQUAL, 1.5),
                ElementCategory.PROPERTY, 2 * numV / 10, new boolean[]{false, sorted});

        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 50).has(LABEL_NAME, "person"),
                ElementCategory.VERTEX, 1, new boolean[]{true, sorted}, vertex1.name());
        evaluateQuery(tx.query().has("text", Cmp.EQUAL, strs[2]).has(LABEL_NAME, "person"),
                ElementCategory.VERTEX, numV / strs.length, new boolean[]{true, sorted}, vertex12.name());
        evaluateQuery(tx.query().has("text", Cmp.EQUAL, strs[3]).has(LABEL_NAME, "person"),
                ElementCategory.VERTEX, 0, new boolean[]{true, sorted}, vertex12.name());
        evaluateQuery(tx.query().has("text", Cmp.EQUAL, strs[2]).has(LABEL_NAME, "person").has("time", Cmp.EQUAL, 2),
                ElementCategory.VERTEX, 1, new boolean[]{true, sorted}, vertex12.name(), vertex1.name());
        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 51).has("name", Cmp.EQUAL, "v51").has(LABEL_NAME, "organization"),
                ElementCategory.VERTEX, 1, new boolean[]{true, sorted}, vertex2.name());
        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 51).has("name", Cmp.EQUAL, "u1").has(LABEL_NAME, "organization"),
                ElementCategory.VERTEX, 1, new boolean[]{true, sorted}, vertex2.name());
        evaluateQuery(tx.query().has("time", Contain.IN, ImmutableList.of(51, 61, 71, 31, 41)).has("name", Cmp.EQUAL, "u1").has(LABEL_NAME, "organization"),
                ElementCategory.VERTEX, 5, new boolean[]{true, sorted}, vertex2.name());
        evaluateQuery(tx.query().has("time", Contain.IN, ImmutableList.of()),
                ElementCategory.VERTEX, 0, new boolean[]{true, false});
        // this query is not fitted because NOT_IN must be filtered in-memory to satisfy has("time")
        evaluateQuery(tx.query().has("text", Cmp.EQUAL, strs[2]).has(LABEL_NAME, "person").has("time", Contain.NOT_IN, ImmutableList.of()),
                ElementCategory.VERTEX, numV / strs.length, new boolean[]{false, sorted}, vertex12.name());


        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 51).has(LABEL_NAME, "organization"),
                ElementCategory.VERTEX, 1, new boolean[]{false, sorted});
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, "u1"),
                ElementCategory.VERTEX, numV / 5, new boolean[]{true, sorted}, vertex3.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, "v1"),
                ElementCategory.VERTEX, 1, new boolean[]{true, sorted}, vertex3.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, "v1").has(LABEL_NAME, "organization"),
                ElementCategory.VERTEX, 1, new boolean[]{false, sorted}, vertex3.name());

        clopen();

        //########## QUERIES (copied from above) ################
        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 10).has("weight", Cmp.EQUAL, 0),
                ElementCategory.EDGE, 1, new boolean[]{true, sorted}, edge1.name());
        evaluateQuery(tx.query().has("time", Contain.IN, ImmutableList.of(10, 20, 30)).has("weight", Cmp.EQUAL, 0),
                ElementCategory.EDGE, 3, new boolean[]{true, sorted}, edge1.name());

        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 10).has("weight", Cmp.EQUAL, 0).has("text", Cmp.EQUAL, strs[10 % strs.length]),
                ElementCategory.EDGE, 1, new boolean[]{false, sorted}, edge1.name());
        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 10).has("weight", Cmp.EQUAL, 1),
                ElementCategory.EDGE, 1, new boolean[]{true, sorted}, edge1.name());
        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 20).has("weight", Cmp.EQUAL, 0),
                ElementCategory.EDGE, 1, new boolean[]{true, sorted}, edge1.name());
        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 20).has("weight", Cmp.EQUAL, 3),
                ElementCategory.EDGE, 0, new boolean[]{true, sorted}, edge1.name());
        evaluateQuery(tx.query().has("text", Cmp.EQUAL, strs[0]).has(LABEL_NAME, "connect"),
                ElementCategory.EDGE, numV / strs.length, new boolean[]{true, sorted}, edge2.name());
        evaluateQuery(tx.query().has("text", Cmp.EQUAL, strs[0]).has(LABEL_NAME, "connect").limit(10),
                ElementCategory.EDGE, 10, new boolean[]{true, sorted}, edge2.name());
        evaluateQuery(tx.query().has("text", Cmp.EQUAL, strs[0]),
                ElementCategory.EDGE, numV / strs.length * 2, new boolean[]{false, sorted});
        evaluateQuery(tx.query().has("weight", Cmp.EQUAL, 1.5),
                ElementCategory.EDGE, numV / 10 * 2, new boolean[]{false, sorted});

        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 50),
                ElementCategory.PROPERTY, 2, new boolean[]{true, sorted}, prop1.name());
        evaluateQuery(tx.query().has("weight", Cmp.EQUAL, 0.0).has("text", Cmp.EQUAL, strs[0]),
                ElementCategory.PROPERTY, 2 * numV / (4 * 5), new boolean[]{true, sorted}, prop2.name());
        evaluateQuery(tx.query().has("weight", Cmp.EQUAL, 0.0).has("text", Cmp.EQUAL, strs[0]).has("time", Cmp.EQUAL, 0),
                ElementCategory.PROPERTY, 2, new boolean[]{true, sorted}, prop2.name(), prop1.name());
        evaluateQuery(tx.query().has("weight", Cmp.EQUAL, 1.5),
                ElementCategory.PROPERTY, 2 * numV / 10, new boolean[]{false, sorted});

        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 50).has(LABEL_NAME, "person"),
                ElementCategory.VERTEX, 1, new boolean[]{true, sorted}, vertex1.name());
        evaluateQuery(tx.query().has("text", Cmp.EQUAL, strs[2]).has(LABEL_NAME, "person"),
                ElementCategory.VERTEX, numV / strs.length, new boolean[]{true, sorted}, vertex12.name());
        evaluateQuery(tx.query().has("text", Cmp.EQUAL, strs[3]).has(LABEL_NAME, "person"),
                ElementCategory.VERTEX, 0, new boolean[]{true, sorted}, vertex12.name());
        evaluateQuery(tx.query().has("text", Cmp.EQUAL, strs[2]).has(LABEL_NAME, "person").has("time", Cmp.EQUAL, 2),
                ElementCategory.VERTEX, 1, new boolean[]{true, sorted}, vertex12.name(), vertex1.name());
        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 51).has("name", Cmp.EQUAL, "v51").has(LABEL_NAME, "organization"),
                ElementCategory.VERTEX, 1, new boolean[]{true, sorted}, vertex2.name());
        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 51).has("name", Cmp.EQUAL, "u1").has(LABEL_NAME, "organization"),
                ElementCategory.VERTEX, 1, new boolean[]{true, sorted}, vertex2.name());
        evaluateQuery(tx.query().has("time", Contain.IN, ImmutableList.of(51, 61, 71, 31, 41)).has("name", Cmp.EQUAL, "u1").has(LABEL_NAME, "organization"),
                ElementCategory.VERTEX, 5, new boolean[]{true, sorted}, vertex2.name());
        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 51).has(LABEL_NAME, "organization"),
                ElementCategory.VERTEX, 1, new boolean[]{false, sorted});
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, "u1"),
                ElementCategory.VERTEX, numV / 5, new boolean[]{true, sorted}, vertex3.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, "v1"),
                ElementCategory.VERTEX, 1, new boolean[]{true, sorted}, vertex3.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, "v1").has(LABEL_NAME, "organization"),
                ElementCategory.VERTEX, 1, new boolean[]{false, sorted}, vertex3.name());
        evaluateQuery(tx.query().has("time", Contain.IN, ImmutableList.of()),
                ElementCategory.VERTEX, 0, new boolean[]{true, false});
        // this query is not fitted because NOT_IN must be filtered in-memory to satisfy has("time")
        evaluateQuery(tx.query().has("text", Cmp.EQUAL, strs[2]).has(LABEL_NAME, "person").has("time", Contain.NOT_IN, ImmutableList.of()),
                ElementCategory.VERTEX, numV / strs.length, new boolean[]{false, sorted}, vertex12.name());

        //Update in transaction
        for (int i = 0; i < numV / 2; i++) {
            JanusGraphVertex v = getV(tx, ns[i]);
            v.remove();
        }
        ns = new JanusGraphVertex[numV * 3 / 2];
        for (int i = numV; i < numV * 3 / 2; i++) {
            ns[i] = tx.addVertex(i % 2 == 0 ? "person" : "organization");
            VertexProperty p1 = ns[i].property("name", "v" + i);
            VertexProperty p2 = ns[i].property("name", "u" + (i % 5));

            double w = (i * 0.5) % 5;
            long t = i;
            String txt = strs[i % (strs.length)];

            ns[i].property(VertexProperty.Cardinality.single, "weight", w);
            ns[i].property(VertexProperty.Cardinality.single, "time", t);
            ns[i].property(VertexProperty.Cardinality.single, "text", txt);

            for (VertexProperty p : new VertexProperty[]{p1, p2}) {
                p.property("weight", w);
                p.property("time", t);
                p.property("text", txt);
            }

            JanusGraphVertex u = ns[(i > numV ? i - 1 : i)]; //previous or self-loop
            for (String label : new String[]{"connect", "related"}) {
                Edge e = ns[i].addEdge(label, u, "weight", (w++) % 5, "time", t, "text", txt);
            }
        }


        //######### UPDATED QUERIES ##########

        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 10).has("weight", Cmp.EQUAL, 0),
                ElementCategory.EDGE, 0, new boolean[]{true, sorted}, edge1.name());
        evaluateQuery(tx.query().has("time", Cmp.EQUAL, numV + 10).has("weight", Cmp.EQUAL, 0),
                ElementCategory.EDGE, 1, new boolean[]{true, sorted}, edge1.name());
        evaluateQuery(tx.query().has("text", Cmp.EQUAL, strs[0]).has(LABEL_NAME, "connect").limit(10),
                ElementCategory.EDGE, 10, new boolean[]{true, sorted}, edge2.name());
        evaluateQuery(tx.query().has("weight", Cmp.EQUAL, 1.5),
                ElementCategory.EDGE, numV / 10 * 2, new boolean[]{false, sorted});

        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 20),
                ElementCategory.PROPERTY, 0, new boolean[]{true, sorted}, prop1.name());
        evaluateQuery(tx.query().has("time", Cmp.EQUAL, numV + 20),
                ElementCategory.PROPERTY, 2, new boolean[]{true, sorted}, prop1.name());

        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 30).has(LABEL_NAME, "person"),
                ElementCategory.VERTEX, 0, new boolean[]{true, sorted}, vertex1.name());
        evaluateQuery(tx.query().has("time", Cmp.EQUAL, numV + 30).has(LABEL_NAME, "person"),
                ElementCategory.VERTEX, 1, new boolean[]{true, sorted}, vertex1.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, "u1"),
                ElementCategory.VERTEX, numV / 5, new boolean[]{true, sorted}, vertex3.name());


        //######### END UPDATED QUERIES ##########

        newTx();

        //######### UPDATED QUERIES (copied from above) ##########
        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 10).has("weight", Cmp.EQUAL, 0),
                ElementCategory.EDGE, 0, new boolean[]{true, sorted}, edge1.name());
        evaluateQuery(tx.query().has("time", Cmp.EQUAL, numV + 10).has("weight", Cmp.EQUAL, 0),
                ElementCategory.EDGE, 1, new boolean[]{true, sorted}, edge1.name());
        evaluateQuery(tx.query().has("text", Cmp.EQUAL, strs[0]).has(LABEL_NAME, "connect").limit(10),
                ElementCategory.EDGE, 10, new boolean[]{true, sorted}, edge2.name());
        evaluateQuery(tx.query().has("weight", Cmp.EQUAL, 1.5),
                ElementCategory.EDGE, numV / 10 * 2, new boolean[]{false, sorted});

        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 20),
                ElementCategory.PROPERTY, 0, new boolean[]{true, sorted}, prop1.name());
        evaluateQuery(tx.query().has("time", Cmp.EQUAL, numV + 20),
                ElementCategory.PROPERTY, 2, new boolean[]{true, sorted}, prop1.name());

        evaluateQuery(tx.query().has("time", Cmp.EQUAL, 30).has(LABEL_NAME, "person"),
                ElementCategory.VERTEX, 0, new boolean[]{true, sorted}, vertex1.name());
        evaluateQuery(tx.query().has("time", Cmp.EQUAL, numV + 30).has(LABEL_NAME, "person"),
                ElementCategory.VERTEX, 1, new boolean[]{true, sorted}, vertex1.name());
        evaluateQuery(tx.query().has("name", Cmp.EQUAL, "u1"),
                ElementCategory.VERTEX, numV / 5, new boolean[]{true, sorted}, vertex3.name());

        //*** INIVIDUAL USE CASE TESTS ******


    }

    @Test
    public void testIndexUniqueness() {
        PropertyKey time = makeKey("time", Long.class);
        PropertyKey text = makeKey("text", String.class);

        VertexLabel person = mgmt.makeVertexLabel("person").make();
        VertexLabel org = mgmt.makeVertexLabel("organization").make();

        JanusGraphIndex vindex1 = mgmt.buildIndex("vindex1", Vertex.class).addKey(time).indexOnly(person).unique().buildCompositeIndex();
        JanusGraphIndex vindex2 = mgmt.buildIndex("vindex2", Vertex.class).addKey(time).addKey(text).unique().buildCompositeIndex();
        finishSchema();

        //================== VERTEX UNIQUENESS ====================

        //I) Label uniqueness
        //Ia) Uniqueness violation in same transaction
        failTransactionOnCommit(new TransactionJob() {
            @Override
            public void run(JanusGraphTransaction tx) {
                JanusGraphVertex v0 = tx.addVertex("person");
                v0.property(VertexProperty.Cardinality.single, "time", 1);
                JanusGraphVertex v1 = tx.addVertex("person");
                v1.property(VertexProperty.Cardinality.single, "time", 1);
            }
        });

        //Ib) Uniqueness violation across transactions
        JanusGraphVertex v0 = tx.addVertex("person");
        v0.property(VertexProperty.Cardinality.single, "time", 1);
        newTx();
        failTransactionOnCommit(new TransactionJob() {
            @Override
            public void run(JanusGraphTransaction tx) {
                JanusGraphVertex v1 = tx.addVertex("person");
                v1.property(VertexProperty.Cardinality.single, "time", 1);
            }
        });
        //Ic) However, this should work since the label is different
        JanusGraphVertex v1 = tx.addVertex("organization");
        v1.property(VertexProperty.Cardinality.single, "time", 1);
        newTx();

        //II) Composite uniqueness
        //IIa) Uniqueness violation in same transaction
        failTransactionOnCommit(new TransactionJob() {
            @Override
            public void run(JanusGraphTransaction tx) {
                JanusGraphVertex v0 = tx.addVertex("time", 2, "text", "hello");
                JanusGraphVertex v1 = tx.addVertex("time", 2, "text", "hello");
            }
        });

        //IIb) Uniqueness violation across transactions
        v0 = tx.addVertex("time", 2, "text", "hello");
        newTx();
        failTransactionOnCommit(new TransactionJob() {
            @Override
            public void run(JanusGraphTransaction tx) {

                JanusGraphVertex v1 = tx.addVertex("time", 2, "text", "hello");
            }
        });


    }

    public static void evaluateQuery(JanusGraphQuery query, ElementCategory resultType,
                                     int expectedResults, boolean[] subQuerySpecs,
                                     PropertyKey orderKey1, Order order1,
                                     String... intersectingIndexes) {
        evaluateQuery(query, resultType, expectedResults, subQuerySpecs, ImmutableMap.of(orderKey1, order1), intersectingIndexes);
    }

    public static void evaluateQuery(JanusGraphQuery query, ElementCategory resultType,
                                     int expectedResults, boolean[] subQuerySpecs,
                                     PropertyKey orderKey1, Order order1, PropertyKey orderKey2, Order order2,
                                     String... intersectingIndexes) {
        evaluateQuery(query, resultType, expectedResults, subQuerySpecs, ImmutableMap.of(orderKey1, order1, orderKey2, order2), intersectingIndexes);
    }

    public static void evaluateQuery(JanusGraphQuery query, ElementCategory resultType,
                                     int expectedResults, boolean[] subQuerySpecs,
                                     String... intersectingIndexes) {
        evaluateQuery(query, resultType, expectedResults, subQuerySpecs, ImmutableMap.<PropertyKey, Order>of(), intersectingIndexes);
    }

    public static void evaluateQuery(JanusGraphQuery query, ElementCategory resultType,
                                     int expectedResults, boolean[] subQuerySpecs,
                                     Map<PropertyKey, Order> orderMap, String... intersectingIndexes) {
        if (intersectingIndexes == null) intersectingIndexes = new String[0];

        SimpleQueryProfiler profiler = new SimpleQueryProfiler();
        ((GraphCentricQueryBuilder) query).profiler(profiler);

        Iterable<? extends JanusGraphElement> result;
        switch (resultType) {
            case PROPERTY:
                result = query.properties();
                break;
            case EDGE:
                result = query.edges();
                break;
            case VERTEX:
                result = query.vertices();
                break;
            default:
                throw new AssertionError();
        }
        OrderList orders = profiler.getAnnotation(QueryProfiler.ORDERS_ANNOTATION);

        //Check elements and that they are returned in the correct order
        int no = 0;
        JanusGraphElement previous = null;
        for (JanusGraphElement e : result) {
            assertNotNull(e);
            no++;
            if (previous != null && !orders.isEmpty()) {
                assertTrue(orders.compare(previous, e) <= 0);
            }
            previous = e;
        }
        assertEquals(expectedResults, no);

        //Check OrderList of query
        assertNotNull(orders);
        assertEquals(orderMap.size(), orders.size());
        for (int i = 0; i < orders.size(); i++) {
            assertEquals(orderMap.get(orders.getKey(i)), orders.getOrder(i));
        }
        for (PropertyKey key : orderMap.keySet()) assertTrue(orders.containsKey(key));

        //Check subqueries
        SimpleQueryProfiler subp = Iterables.getOnlyElement(Iterables.filter(profiler, p -> !p.getGroupName().equals(QueryProfiler.OPTIMIZATION)));
        if (subQuerySpecs.length == 2) { //0=>fitted, 1=>ordered
            assertEquals(subQuerySpecs[0], subp.getAnnotation(QueryProfiler.FITTED_ANNOTATION));
            assertEquals(subQuerySpecs[1], subp.getAnnotation(QueryProfiler.ORDERED_ANNOTATION));
        }
        Set<String> indexNames = new HashSet<>();
        int indexQueries = 0;
        boolean fullscan = false;
        for (SimpleQueryProfiler indexp : subp) {
            if (indexp.getAnnotation(QueryProfiler.FULLSCAN_ANNOTATION) != null) {
                fullscan = true;
            } else {
                indexNames.add(indexp.getAnnotation(QueryProfiler.INDEX_ANNOTATION));
                indexQueries++;
            }
        }
        if (indexQueries > 0) assertFalse(fullscan);
        if (fullscan) assertTrue(intersectingIndexes.length == 0);
        assertEquals(intersectingIndexes.length, indexQueries);
        assertEquals(Sets.newHashSet(intersectingIndexes), indexNames);
    }

    @Test
    public void testForceIndexUsage() {
        PropertyKey age = makeKey("age", Integer.class);
        PropertyKey time = makeKey("time", Long.class);
        mgmt.buildIndex("time", Vertex.class).addKey(time).buildCompositeIndex();
        finishSchema();

        for (int i = 1; i <= 10; i++) {
            JanusGraphVertex v = tx.addVertex("time", i, "age", i);
        }

        //Graph query with and with-out index support
        assertCount(1, tx.query().has("time", 5).vertices());
        assertCount(1, tx.query().has("age", 6).vertices());

        clopen(option(FORCE_INDEX_USAGE), true);
        //Query with-out index support should now throw exception
        assertCount(1, tx.query().has("time", 5).vertices());
        try {
            assertCount(1, tx.query().has("age", 6).vertices());
            fail();
        } catch (Exception e) {
        }
    }

    @Test
    public void testLargeJointIndexRetrieval() {
        makeVertexIndexedKey("sid", Integer.class);
        makeVertexIndexedKey("color", String.class);
        finishSchema();

        int sids = 17;
        String[] colors = {"blue", "red", "yellow", "brown", "green", "orange", "purple"};
        int multiplier = 200;
        int numV = sids * colors.length * multiplier;
        for (int i = 0; i < numV; i++) {
            JanusGraphVertex v = graph.addVertex(
                    "color", colors[i % colors.length],
                    "sid", i % sids);
        }
        clopen();

        assertCount(numV / sids, graph.query().has("sid", 8).vertices());
        assertCount(numV / colors.length, graph.query().has("color", colors[2]).vertices());

        assertCount(multiplier, graph.query().has("sid", 11).has("color", colors[3]).vertices());
    }


    @Test
    public void testIndexQueryWithLabelsAndContainsIN() {
        // This test is based on the steps to reproduce #882

        String labelName = "labelName";

        VertexLabel label = mgmt.makeVertexLabel(labelName).make();
        PropertyKey uid = mgmt.makePropertyKey("uid").dataType(String.class).make();
        JanusGraphIndex uidCompositeIndex = mgmt.buildIndex("uidIndex", Vertex.class).indexOnly(label).addKey(uid).unique().buildCompositeIndex();
        mgmt.setConsistency(uidCompositeIndex, ConsistencyModifier.LOCK);
        finishSchema();

        JanusGraphVertex foo = graph.addVertex(labelName);
        JanusGraphVertex bar = graph.addVertex(labelName);
        foo.property("uid", "foo");
        bar.property("uid", "bar");
        graph.tx().commit();

        Iterable<JanusGraphVertex> vertexes = graph.query()
                .has("uid", Contain.IN, ImmutableList.of("foo", "bar"))
                .has(LABEL_NAME, labelName)
                .vertices();
        assertEquals(2, Iterables.size(vertexes));
        for (JanusGraphVertex v : vertexes) {
            assertEquals(labelName, v.vertexLabel().name());
        }
    }

    @Test
    public void testLimitWithMixedIndexCoverage() {
        final String vt = "vt";
        final String fn = "firstname";
        final String user = "user";
        final String alice = "alice";
        final String bob = "bob";

        PropertyKey vtk = makeVertexIndexedKey(vt, String.class);
        PropertyKey fnk = makeKey(fn, String.class);

        finishSchema();

        JanusGraphVertex a = tx.addVertex(vt, user, fn, "alice");
        JanusGraphVertex b = tx.addVertex(vt, user, fn, "bob");
        JanusGraphVertex v;

        v = Iterables.<JanusGraphVertex>getOnlyElement(tx.query().has(vt, user).has(fn, bob).limit(1).vertices());
        assertEquals(bob, v.value(fn));
        assertEquals(user, v.value(vt));

        v = Iterables.<JanusGraphVertex>getOnlyElement(tx.query().has(vt, user).has(fn, alice).limit(1).vertices());
        assertEquals(alice, v.value(fn));
        assertEquals(user, v.value(vt));

        tx.commit();
        tx = graph.newTransaction();

        v = Iterables.<JanusGraphVertex>getOnlyElement(tx.query().has(vt, user).has(fn, bob).limit(1).vertices());
        assertEquals(bob, v.value(fn));
        assertEquals(user, v.value(vt));

        v = Iterables.<JanusGraphVertex>getOnlyElement(tx.query().has(vt, user).has(fn, alice).limit(1).vertices());
        assertEquals(alice, v.value(fn));
        assertEquals(user, v.value(vt));
    }

    @Test
    public void testWithoutIndex() {
        PropertyKey kid = mgmt.makePropertyKey("kid").dataType(Long.class).make();
        mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.makeEdgeLabel("knows").signature(kid).make();
        finishSchema();

        Random random = new Random();
        int numV = 1000;
        JanusGraphVertex previous = null;
        for (int i = 0; i < numV; i++) {
            JanusGraphVertex v = graph.addVertex(
                    "kid", random.nextInt(numV), "name", "v" + i);
            if (previous != null) {
                Edge e = v.addEdge("knows", previous, "kid", random.nextInt(numV / 2));
            }
            previous = v;
        }

        verifyElementOrder(graph.query().orderBy("kid", incr).limit(500).vertices(), "kid", Order.ASC, 500);
        verifyElementOrder(graph.query().orderBy("kid", incr).limit(300).edges(), "kid", Order.ASC, 300);
        verifyElementOrder(graph.query().orderBy("kid", decr).limit(400).vertices(), "kid", Order.DESC, 400);
        verifyElementOrder(graph.query().orderBy("kid", decr).limit(200).edges(), "kid", Order.DESC, 200);

        clopen();

        //Copied from above
        verifyElementOrder(graph.query().orderBy("kid", incr).limit(500).vertices(), "kid", Order.ASC, 500);
        verifyElementOrder(graph.query().orderBy("kid", incr).limit(300).edges(), "kid", Order.ASC, 300);
        verifyElementOrder(graph.query().orderBy("kid", decr).limit(400).vertices(), "kid", Order.DESC, 400);
        verifyElementOrder(graph.query().orderBy("kid", decr).limit(200).edges(), "kid", Order.DESC, 200);
    }


    //................................................


    @Test
    public void testHasNot() {
        JanusGraphVertex v1, v2;
        v1 = graph.addVertex();

        v2 = (JanusGraphVertex) graph.query().hasNot("abcd").vertices().iterator().next();
        assertEquals(v1, v2);
        v2 = (JanusGraphVertex) graph.query().hasNot("abcd", true).vertices().iterator().next();
        assertEquals(v1, v2);
    }

    @Test
    public void testVertexCentricIndexWithNull() {
        EdgeLabel bought = makeLabel("bought");
        PropertyKey time = makeKey("time", Long.class);
        mgmt.buildEdgeIndex(bought, "byTimeDesc", BOTH, decr, time);
        mgmt.buildEdgeIndex(bought, "byTimeIncr", BOTH, incr, time);
        finishSchema();

        JanusGraphVertex v1 = tx.addVertex(), v2 = tx.addVertex();
        v1.addEdge("bought", v2).property("time", 1);
        v1.addEdge("bought", v2).property("time", 2);
        v1.addEdge("bought", v2).property("time", 3);
        v1.addEdge("bought", v2);
        v1.addEdge("bought", v2);

        assertEquals(5, v1.query().direction(OUT).labels("bought").edgeCount());
        assertEquals(1, v1.query().direction(OUT).labels("bought").has("time", 1).edgeCount());
        assertEquals(1, v1.query().direction(OUT).labels("bought").has("time", Cmp.LESS_THAN, 3).has("time", Cmp.GREATER_THAN, 1).edgeCount());
        assertEquals(3, v1.query().direction(OUT).labels("bought").has("time", Cmp.LESS_THAN, 5).edgeCount());
        assertEquals(3, v1.query().direction(OUT).labels("bought").has("time", Cmp.GREATER_THAN, 0).edgeCount());
        assertEquals(2, v1.query().direction(OUT).labels("bought").has("time", Cmp.LESS_THAN, 3).edgeCount());
        assertEquals(1, v1.query().direction(OUT).labels("bought").has("time", Cmp.GREATER_THAN, 2).edgeCount());
        assertEquals(2, v1.query().direction(OUT).labels("bought").hasNot("time").edgeCount());
        assertEquals(5, v1.query().direction(OUT).labels("bought").edgeCount());


        newTx();
        v1 = tx.getVertex(v1.longId());
        //Queries copied from above

        assertEquals(5, v1.query().direction(OUT).labels("bought").edgeCount());
        assertEquals(1, v1.query().direction(OUT).labels("bought").has("time", 1).edgeCount());
        assertEquals(1, v1.query().direction(OUT).labels("bought").has("time", Cmp.LESS_THAN, 3).has("time", Cmp.GREATER_THAN, 1).edgeCount());
        assertEquals(3, v1.query().direction(OUT).labels("bought").has("time", Cmp.LESS_THAN, 5).edgeCount());
        assertEquals(3, v1.query().direction(OUT).labels("bought").has("time", Cmp.GREATER_THAN, 0).edgeCount());
        assertEquals(2, v1.query().direction(OUT).labels("bought").has("time", Cmp.LESS_THAN, 3).edgeCount());
        assertEquals(1, v1.query().direction(OUT).labels("bought").has("time", Cmp.GREATER_THAN, 2).edgeCount());
        assertEquals(2, v1.query().direction(OUT).labels("bought").hasNot("time").edgeCount());
        assertEquals(5, v1.query().direction(OUT).labels("bought").edgeCount());
    }

    //Add more removal operations, different transaction contexts
    @Test
    public void testCreateDelete() {
        makeKey("weight", Double.class);
        PropertyKey uid = makeVertexIndexedUniqueKey("uid", Integer.class);
        ((StandardEdgeLabelMaker) mgmt.makeEdgeLabel("knows")).sortKey(uid).sortOrder(Order.DESC).directed().make();
        mgmt.makeEdgeLabel("father").multiplicity(Multiplicity.MANY2ONE).make();
        finishSchema();

        JanusGraphVertex v1 = graph.addVertex(), v3 = graph.addVertex("uid", 445);
        Edge e = v3.addEdge("knows", v1, "uid", 111);
        Edge e2 = v1.addEdge("friend", v3);
        assertEquals(111, e.<Integer>value("uid").intValue());
        graph.tx().commit();

        v3 = getV(graph, v3);
        assertEquals(445, v3.<Integer>value("uid").intValue());
        e = Iterables.<JanusGraphEdge>getOnlyElement(v3.query().direction(Direction.OUT).labels("knows").edges());
        assertEquals(111, e.<Integer>value("uid").intValue());
        assertEquals(e, getE(graph, e.id()));
        assertEquals(e, getE(graph, e.id().toString()));
        VertexProperty p = getOnlyElement(v3.properties("uid"));
        p.remove();
        v3.property("uid", 353);

        e = Iterables.<JanusGraphEdge>getOnlyElement(v3.query().direction(Direction.OUT).labels("knows").edges());
        e.property("uid", 222);

        e2 = Iterables.<JanusGraphEdge>getOnlyElement(v1.query().direction(Direction.OUT).labels("friend").edges());
        e2.property("uid", 1);
        e2.property("weight", 2.0);

        assertEquals(1, e2.<Integer>value("uid").intValue());
        assertEquals(2.0, e2.<Double>value("weight").doubleValue(), 0.0001);


        clopen();

        v3 = getV(graph, v3.id());
        assertEquals(353, v3.<Integer>value("uid").intValue());

        e = Iterables.<JanusGraphEdge>getOnlyElement(v3.query().direction(Direction.OUT).labels("knows").edges());
        assertEquals(222, e.<Integer>value("uid").intValue());
    }

   /* ==================================================================================
                            TIME TO LIVE
     ==================================================================================*/

    @Test
    public void testEdgeTTLTiming() throws Exception {
        if (!features.hasCellTTL()) {
            return;
        }

        EdgeLabel label1 = mgmt.makeEdgeLabel("likes").make();
        int ttl1 = 1;
        int ttl2 = 4;
        mgmt.setTTL(label1, Duration.ofSeconds(ttl1));
        EdgeLabel label2 = mgmt.makeEdgeLabel("dislikes").make();
        mgmt.setTTL(label2, Duration.ofSeconds(ttl2));
        EdgeLabel label3 = mgmt.makeEdgeLabel("indifferentTo").make();
        assertEquals(Duration.ofSeconds(ttl1), mgmt.getTTL(label1));
        assertEquals(Duration.ofSeconds(ttl2), mgmt.getTTL(label2));
        assertEquals(Duration.ZERO, mgmt.getTTL(label3));
        mgmt.commit();

        JanusGraphVertex v1 = graph.addVertex(), v2 = graph.addVertex(), v3 = graph.addVertex();

        v1.addEdge("likes", v2);
        v2.addEdge("dislikes", v1);
        v3.addEdge("indifferentTo", v1);

        // initial, pre-commit state of the edges.  They are not yet subject to TTL
        assertNotEmpty(v1.query().direction(Direction.OUT).vertices());
        assertNotEmpty(v2.query().direction(Direction.OUT).vertices());
        assertNotEmpty(v3.query().direction(Direction.OUT).vertices());

        long commitTime = System.currentTimeMillis();
        graph.tx().commit();

        // edges are now subject to TTL, although we must commit() or rollback() to see it
        assertNotEmpty(v1.query().direction(Direction.OUT).vertices());
        assertNotEmpty(v2.query().direction(Direction.OUT).vertices());
        assertNotEmpty(v3.query().direction(Direction.OUT).vertices());

        Thread.sleep(commitTime + (ttl1 * 1000L + 200) - System.currentTimeMillis());
        graph.tx().rollback();

        // e1 has dropped out
        assertEmpty(v1.query().direction(Direction.OUT).vertices());
        assertNotEmpty(v2.query().direction(Direction.OUT).vertices());
        assertNotEmpty(v3.query().direction(Direction.OUT).vertices());

        Thread.sleep(commitTime + (ttl2 * 1000L + 500) - System.currentTimeMillis());
        graph.tx().rollback();

        // both e1 and e2 have dropped out.  e3 has no TTL, and so remains
        assertEmpty(v1.query().direction(Direction.OUT).vertices());
        assertEmpty(v2.query().direction(Direction.OUT).vertices());
        assertNotEmpty(v3.query().direction(Direction.OUT).vertices());
    }

    @Test
    public void testEdgeTTLWithTransactions() throws Exception {
        if (!features.hasCellTTL()) {
            return;
        }

        EdgeLabel label1 = mgmt.makeEdgeLabel("likes").make();
        mgmt.setTTL(label1, Duration.ofSeconds(1));
        assertEquals(Duration.ofSeconds(1), mgmt.getTTL(label1));
        mgmt.commit();

        JanusGraphVertex v1 = graph.addVertex(), v2 = graph.addVertex();

        v1.addEdge("likes", v2);

        // pre-commit state of the edge.  It is not yet subject to TTL
        assertNotEmpty(v1.query().direction(Direction.OUT).vertices());

        Thread.sleep(1001);

        // the edge should have expired by now, but only if it had been committed
        assertNotEmpty(v1.query().direction(Direction.OUT).vertices());

        graph.tx().commit();

        // still here, because we have just committed the edge.  Its countdown starts at the commit
        assertNotEmpty(v1.query().direction(Direction.OUT).vertices());

        Thread.sleep(1001);

        // the edge has expired in Cassandra, but still appears alive in this transaction
        assertNotEmpty(v1.query().direction(Direction.OUT).vertices());

        // syncing with the data store, we see that the edge has expired
        graph.tx().rollback();
        assertEmpty(v1.query().direction(Direction.OUT).vertices());
    }

    @Category({BrittleTests.class})
    @Test
    public void testEdgeTTLWithIndex() throws Exception {
        if (!features.hasCellTTL()) {
            return;
        }

        int ttl = 1; // artificially low TTL for test
        final PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).make();
        EdgeLabel wavedAt = mgmt.makeEdgeLabel("wavedAt").signature(time).make();
        mgmt.buildEdgeIndex(wavedAt, "timeindex", Direction.BOTH, decr, time);
        mgmt.buildIndex("edge-time", Edge.class).addKey(time).buildCompositeIndex();
        mgmt.setTTL(wavedAt, Duration.ofSeconds(ttl));
        assertEquals(Duration.ZERO, mgmt.getTTL(time));
        assertEquals(Duration.ofSeconds(ttl), mgmt.getTTL(wavedAt));
        mgmt.commit();

        JanusGraphVertex v1 = graph.addVertex(), v2 = graph.addVertex();
        v1.addEdge("wavedAt", v2, "time", 42);

        assertTrue(v1.query().direction(Direction.OUT).interval("time", 0, 100).edges().iterator().hasNext());
        assertNotEmpty(v1.query().direction(Direction.OUT).edges());
        assertNotEmpty(graph.query().has("time", 42).edges());

        graph.tx().commit();
        long commitTime = System.currentTimeMillis();

        assertTrue(v1.query().direction(Direction.OUT).interval("time", 0, 100).edges().iterator().hasNext());
        assertNotEmpty(v1.query().direction(Direction.OUT).edges());
        assertNotEmpty(graph.query().has("time", 42).edges());

        Thread.sleep(commitTime + (ttl * 1000L + 100) - System.currentTimeMillis());
        graph.tx().rollback();

        assertFalse(v1.query().direction(Direction.OUT).interval("time", 0, 100).edges().iterator().hasNext());
        assertEmpty(v1.query().direction(Direction.OUT).edges());
        assertEmpty(graph.query().has("time", 42).edges());
    }

    @Category({BrittleTests.class})
    @Test
    public void testPropertyTTLTiming() throws Exception {
        if (!features.hasCellTTL()) {
            return;
        }

        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        PropertyKey place = mgmt.makePropertyKey("place").dataType(String.class).make();
        mgmt.setTTL(name, Duration.ofSeconds(42));
        mgmt.setTTL(place, Duration.ofSeconds(1));
        JanusGraphIndex index1 = mgmt.buildIndex("index1", Vertex.class).addKey(name).buildCompositeIndex();
        JanusGraphIndex index2 = mgmt.buildIndex("index2", Vertex.class).addKey(name).addKey(place).buildCompositeIndex();
        VertexLabel label1 = mgmt.makeVertexLabel("event").setStatic().make();
        mgmt.setTTL(label1, Duration.ofSeconds(2));
        assertEquals(Duration.ofSeconds(42), mgmt.getTTL(name));
        assertEquals(Duration.ofSeconds(1), mgmt.getTTL(place));
        assertEquals(Duration.ofSeconds(2), mgmt.getTTL(label1));
        mgmt.commit();

        JanusGraphVertex v1 = tx.addVertex(T.label, "event", "name", "some event", "place", "somewhere");

        tx.commit();
        Object id = v1.id();

        v1 = getV(graph, id);
        assertNotNull(v1);
        assertNotEmpty(graph.query().has("name", "some event").has("place", "somewhere").vertices());
        assertNotEmpty(graph.query().has("name", "some event").vertices());

        Thread.sleep(1001);
        graph.tx().rollback();

        // short-lived property expires first
        v1 = getV(graph, id);
        assertNotNull(v1);
        assertEmpty(graph.query().has("name", "some event").has("place", "somewhere").vertices());
        assertNotEmpty(graph.query().has("name", "some event").vertices());

        Thread.sleep(1001);
        graph.tx().rollback();

        // vertex expires before defined TTL of the long-lived property
        assertEmpty(graph.query().has("name", "some event").has("place", "somewhere").vertices());
        assertEmpty(graph.query().has("name", "some event").vertices());
        v1 = getV(graph, id);
        assertNull(v1);
    }

    @Test
    public void testVertexTTLWithCompositeIndex() throws Exception {
        if (!features.hasCellTTL()) {
            return;
        }

        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        PropertyKey time = mgmt.makePropertyKey("time").dataType(Long.class).make();
        JanusGraphIndex index1 = mgmt.buildIndex("index1", Vertex.class).addKey(name).buildCompositeIndex();
        JanusGraphIndex index2 = mgmt.buildIndex("index2", Vertex.class).addKey(name).addKey(time).buildCompositeIndex();
        VertexLabel label1 = mgmt.makeVertexLabel("event").setStatic().make();
        mgmt.setTTL(label1, Duration.ofSeconds(1));
        assertEquals(Duration.ZERO, mgmt.getTTL(name));
        assertEquals(Duration.ZERO, mgmt.getTTL(time));
        assertEquals(Duration.ofSeconds(1), mgmt.getTTL(label1));
        mgmt.commit();

        JanusGraphVertex v1 = tx.addVertex(T.label, "event", "name", "some event", "time", System.currentTimeMillis());
        tx.commit();
        Object id = v1.id();

        v1 = getV(graph, id);
        assertNotNull(v1);
        assertNotEmpty(graph.query().has("name", "some event").vertices());

        Thread.sleep(1001);
        graph.tx().rollback();

        v1 = getV(graph, id);
        assertNull(v1);
        assertEmpty(graph.query().has("name", "some event").vertices());
    }

    @Category({BrittleTests.class})
    @Test
    public void testEdgeTTLLimitedByVertexTTL() throws Exception {

        if (!features.hasCellTTL()) {
            return;
        }
        Boolean dbCache = config.get("cache.db-cache", Boolean.class);
        if (null == dbCache) {
            dbCache = false;
        }

        EdgeLabel likes = mgmt.makeEdgeLabel("likes").make();
        mgmt.setTTL(likes, Duration.ofSeconds(42)); // long edge TTL will be overridden by short vertex TTL
        EdgeLabel dislikes = mgmt.makeEdgeLabel("dislikes").make();
        mgmt.setTTL(dislikes, Duration.ofSeconds(1));
        EdgeLabel indifferentTo = mgmt.makeEdgeLabel("indifferentTo").make();
        VertexLabel label1 = mgmt.makeVertexLabel("person").setStatic().make();
        mgmt.setTTL(label1, Duration.ofSeconds(2));
        assertEquals(Duration.ofSeconds(42), mgmt.getTTL(likes));
        assertEquals(Duration.ofSeconds(1), mgmt.getTTL(dislikes));
        assertEquals(Duration.ZERO, mgmt.getTTL(indifferentTo));
        assertEquals(Duration.ofSeconds(2), mgmt.getTTL(label1));
        mgmt.commit();

        JanusGraphVertex v1 = tx.addVertex("person");
        JanusGraphVertex v2 = tx.addVertex();
        Edge v1LikesV2 = v1.addEdge("likes", v2);
        Edge v1DislikesV2 = v1.addEdge("dislikes", v2);
        Edge v1IndifferentToV2 = v1.addEdge("indifferentTo", v2);
        tx.commit();
        long commitTime = System.currentTimeMillis();

        Object v1Id = v1.id();
        Object v2id = v2.id();
        Object v1LikesV2Id = v1LikesV2.id();
        Object v1DislikesV2Id = v1DislikesV2.id();
        Object v1IndifferentToV2Id = v1IndifferentToV2.id();

        v1 = getV(graph, v1Id);
        v2 = getV(graph, v2id);
        v1LikesV2 = getE(graph, v1LikesV2Id);
        v1DislikesV2 = getE(graph, v1DislikesV2Id);
        v1IndifferentToV2 = getE(graph, v1IndifferentToV2Id);
        assertNotNull(v1);
        assertNotNull(v2);
        assertNotNull(v1LikesV2);
        assertNotNull(v1DislikesV2);
        assertNotNull(v1IndifferentToV2);
        assertNotEmpty(v2.query().direction(Direction.IN).labels("likes").edges());
        assertNotEmpty(v2.query().direction(Direction.IN).labels("dislikes").edges());
        assertNotEmpty(v2.query().direction(Direction.IN).labels("indifferentTo").edges());

        Thread.sleep(commitTime + 1001L - System.currentTimeMillis());
        graph.tx().rollback();

        v1 = getV(graph, v1Id);
        v2 = getV(graph, v2id);
        v1LikesV2 = getE(graph, v1LikesV2Id);
        v1DislikesV2 = getE(graph, v1DislikesV2Id);
        v1IndifferentToV2 = getE(graph, v1IndifferentToV2Id);
        assertNotNull(v1);
        assertNotNull(v2);
        assertNotNull(v1LikesV2);
        // this edge has expired
        assertNull(v1DislikesV2);
        assertNotNull(v1IndifferentToV2);
        assertNotEmpty(v2.query().direction(Direction.IN).labels("likes").edges());
        // expired
        assertEmpty(v2.query().direction(Direction.IN).labels("dislikes").edges());
        assertNotEmpty(v2.query().direction(Direction.IN).labels("indifferentTo").edges());

        Thread.sleep(commitTime + 2001L - System.currentTimeMillis());
        graph.tx().rollback();

        v1 = getV(graph, v1Id);
        v2 = getV(graph, v2id);
        v1LikesV2 = getE(graph, v1LikesV2Id);
        v1DislikesV2 = getE(graph, v1DislikesV2Id);
        v1IndifferentToV2 = getE(graph, v1IndifferentToV2Id);
        // the vertex itself has expired
        assertNull(v1);
        assertNotNull(v2);
        // all incident edges have necessarily expired
        assertNull(v1LikesV2);
        assertNull(v1DislikesV2);
        assertNull(v1IndifferentToV2);

        if (dbCache) {
            /* TODO: uncomment
            assertNotEmpty(v2.query().direction(Direction.IN).labels("likes").edges());
            assertNotEmpty(v2.query().direction(Direction.IN).labels("dislikes").edges());
            assertNotEmpty(v2.query().direction(Direction.IN).labels("indifferentTo").edges());
            */
        } else {
            assertEmpty(v2.query().direction(Direction.IN).labels("likes").edges());
            assertEmpty(v2.query().direction(Direction.IN).labels("dislikes").edges());
            assertEmpty(v2.query().direction(Direction.IN).labels("indifferentTo").edges());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSettingTTLOnUnsupportedType() throws Exception {
        if (!features.hasCellTTL()) {
            throw new IllegalArgumentException();
        }

        JanusGraphSchemaType type = ImplicitKey.ID;
        mgmt.setTTL(type, Duration.ZERO);
    }

    @Test
    public void testUnsettingTTL() throws InterruptedException {
        if (!features.hasCellTTL()) {
            return;
        }

        int initialTTLMillis = 2000;

        // Define schema: one edge label with a short ttl
        EdgeLabel likes = mgmt.makeEdgeLabel("likes").make();
        mgmt.setTTL(likes, Duration.ofMillis(initialTTLMillis));
        mgmt.commit();
        graph.tx().rollback();

        // Insert two vertices with a TTLed edge
        JanusGraphVertex v1 = graph.addVertex();
        JanusGraphVertex v2 = graph.addVertex();
        v1.addEdge("likes", v2);
        graph.tx().commit();

        // Let the edge die
        Thread.sleep((long) Math.ceil(initialTTLMillis * 1.25));

        // Edge should be gone
        assertEquals(2, Iterators.size(graph.vertices()));
        assertEquals(0, Iterators.size(graph.edges()));
        graph.tx().rollback();

        // Remove the TTL on the edge label
        mgmt = graph.openManagement();
        mgmt.setTTL(mgmt.getEdgeLabel("likes"), Duration.ZERO);
        mgmt.commit();

        Thread.sleep(1L);

        // Check that the edge is still gone, add a new edge
        assertEquals(2, Iterators.size(graph.vertices()));
        assertEquals(0, Iterators.size(graph.edges()));
        v1 = graph.addVertex();
        v2 = graph.addVertex();
        v1.addEdge("likes", v2);
        graph.tx().commit();

        // Sleep past when it would have expired under the original config
        Thread.sleep((long) Math.ceil(initialTTLMillis * 1.25));

        // Edge must not be dead
        assertEquals(4, Iterators.size(graph.vertices()));
        assertEquals(1, Iterators.size(graph.edges()));
        graph.tx().rollback();
    }

    @Test
    public void testGettingUndefinedEdgeLabelTTL() {
        if (!features.hasCellTTL()) {
            return;
        }

        // getTTL should return a null duration on an extant type without a TTL
        mgmt.makeEdgeLabel("likes").make();
        mgmt.commit();
        graph.tx().rollback();

        // Check getTTL on edge label
        mgmt = graph.openManagement();
        assertEquals(Duration.ZERO, mgmt.getTTL(mgmt.getEdgeLabel("likes")));
        mgmt.rollback();
    }

    @Test
    public void testGettingUndefinedVertexLabelTTL() {
        if (!features.hasCellTTL()) {
            return;
        }

        // getTTL should return a null duration on an extant type without a TTL
        mgmt.makeVertexLabel("foo").make();
        mgmt.commit();
        graph.tx().rollback();

        // Check getTTL on vertex label
        mgmt = graph.openManagement();
        assertEquals(Duration.ZERO, mgmt.getTTL(mgmt.getVertexLabel("foo")));
        mgmt.rollback();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTTLFromUnsupportedType() throws Exception {
        if (!features.hasCellTTL()) {
            throw new IllegalArgumentException();
        }

        JanusGraphSchemaType type = ImplicitKey.ID;
        mgmt.getTTL(type);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSettingTTLOnNonStaticVertexLabel() throws Exception {
        if (!features.hasCellTTL()) {
            throw new IllegalArgumentException();
        }

        VertexLabel label1 = mgmt.makeVertexLabel("event").make();
        mgmt.setTTL(label1, Duration.ofSeconds(42));
    }

    @Test
    public void testEdgeTTLImplicitKey() throws Exception {
        Duration d;

        if (!features.hasCellTTL()) {
            return;
        }

        clopen(option(GraphDatabaseConfiguration.STORE_META_TTL, "edgestore"), true);

        assertEquals("~ttl", ImplicitKey.TTL.name());

        int ttl = 24 * 60 * 60;
        EdgeLabel likes = mgmt.makeEdgeLabel("likes").make();
        EdgeLabel hasLiked = mgmt.makeEdgeLabel("hasLiked").make();
        mgmt.setTTL(likes, Duration.ofSeconds(ttl));
        assertEquals(Duration.ofSeconds(ttl), mgmt.getTTL(likes));
        assertEquals(Duration.ZERO, mgmt.getTTL(hasLiked));
        mgmt.commit();

        JanusGraphVertex v1 = graph.addVertex(), v2 = graph.addVertex();

        Edge e1 = v1.addEdge("likes", v2);
        Edge e2 = v1.addEdge("hasLiked", v2);
        graph.tx().commit();

        // read from the edge created in this transaction
        d = e1.value("~ttl");
        assertEquals(Duration.ofDays(1), d);

        // get the edge via a vertex
        e1 = Iterables.<JanusGraphEdge>getOnlyElement(v1.query().direction(Direction.OUT).labels("likes").edges());
        d = e1.value("~ttl");
        assertEquals(Duration.ofDays(1), d);

        // returned value of ^ttl is the total time to live since commit, not remaining time
        Thread.sleep(1001);
        graph.tx().rollback();
        e1 = Iterables.<JanusGraphEdge>getOnlyElement(v1.query().direction(Direction.OUT).labels("likes").edges());
        d = e1.value("~ttl");
        assertEquals(Duration.ofDays(1), d);

        // no ttl on edges of this label
        d = e2.value("~ttl");
        assertEquals(Duration.ZERO, d);
    }

    @Test
    public void testVertexTTLImplicitKey() throws Exception {
        Duration d;

        if (!features.hasCellTTL()) {
            return;
        }

        clopen(option(GraphDatabaseConfiguration.STORE_META_TTL, "edgestore"), true);

        int ttl1 = 1;
        VertexLabel label1 = mgmt.makeVertexLabel("event").setStatic().make();
        mgmt.setTTL(label1, Duration.ofSeconds(ttl1));
        assertEquals(Duration.ofSeconds(ttl1), mgmt.getTTL(label1));
        mgmt.commit();

        JanusGraphVertex v1 = tx.addVertex("event");
        JanusGraphVertex v2 = tx.addVertex();
        tx.commit();

        /* TODO: this fails
        d = v1.getProperty("~ttl");
        assertEquals(1, d);
        d = v2.getProperty("~ttl");
        assertEquals(0, d);
        */

        Object v1id = v1.id();
        Object v2id = v2.id();
        v1 = getV(graph, v1id);
        v2 = getV(graph, v2id);

        d = v1.value("~ttl");
        assertEquals(Duration.ofSeconds(1), d);
        d = v2.value("~ttl");
        assertEquals(Duration.ZERO, d);
    }

    @Test
    public void testAutoSchemaMakerForVertexPropertyDataType(){
        JanusGraphVertex v1 = tx.addVertex("user");
        v1.property("id", 10);
        v1.property("created", new Date());

        PropertyKey idPropertyKey = tx.getPropertyKey("id");
        assertEquals("Data type not identified correctly by auto schema maker",
            Integer.class, idPropertyKey.dataType());
        PropertyKey createdPropertyKey = tx.getPropertyKey("created");
        assertEquals("Data type not identified properly by auto schema maker",
            Date.class, createdPropertyKey.dataType());

    }

    @Test
    public void testAutoSchemaMakerForEdgePropertyDataType(){
        JanusGraphVertex v1 = tx.addVertex("user");
        JanusGraphVertex v2 = tx.addVertex("user");
        v1.addEdge("knows", v2, "id", 10, "created", new Date());

        PropertyKey idPropertyKey = tx.getPropertyKey("id");
        assertEquals("Data type not identified correctly by auto schema maker",
            Integer.class, idPropertyKey.dataType());
        PropertyKey createdPropertyKey = tx.getPropertyKey("created");
        assertEquals("Data type not identified correctly by auto schema maker",
            Date.class, createdPropertyKey.dataType());
    }

}
