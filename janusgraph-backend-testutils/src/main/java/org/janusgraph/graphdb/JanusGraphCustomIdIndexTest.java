// Copyright 2022 JanusGraph Authors
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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.core.util.ManagementUtil;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.indexing.IndexFeatures;
import org.janusgraph.diskstorage.log.kcvs.KCVSLog;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.types.ParameterType;
import org.janusgraph.testutil.TestGraphConfigs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ALLOW_SETTING_VERTEX_ID;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ALLOW_CUSTOM_VERTEX_ID_TYPES;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.FORCE_INDEX_USAGE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_READ_INTERVAL;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_SEND_DELAY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.MANAGEMENT_LOG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * This test suite is responsible for testing custom vertex id assignment with
 * an external index backend
 */
public abstract class JanusGraphCustomIdIndexTest extends JanusGraphBaseTest {

    private static final Logger log = LoggerFactory.getLogger(JanusGraphCustomIdIndexTest.class);

    private static final String INDEX = "search";
    private static final String INDEX2 = "search2";

    private IndexFeatures indexFeatures;

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

    private void open(boolean allowSettingVertexId, boolean allowCustomVertexIdType) {
        ModifiableConfiguration config = getModifiableConfiguration();
        config.set(ALLOW_SETTING_VERTEX_ID, allowSettingVertexId, new String[0]);
        config.set(ALLOW_CUSTOM_VERTEX_ID_TYPES, allowCustomVertexIdType, new String[0]);
        open(config.getConfiguration());
        indexFeatures = graph.getBackend().getIndexFeatures().get(INDEX);
    }

    protected String[] getIndexBackends() {
        return new String[]{INDEX, INDEX2};
    }

    private String getRandomId() {
        return UUID.randomUUID().toString().replace('-', '_');
    }

    private void addVertex(int time, String text, double height, String[] phones) {
        newTx();
        final JanusGraphVertex v = tx.addVertex(T.id, getRandomId(), "text", text, "time", time, "height", height);
        for (final String phone : phones) {
            v.property("phone", phone);
        }
        newTx();
    }

    private Parameter getTextMapping() {
        if (indexFeatures.supportsStringMapping(Mapping.TEXT)) return Mapping.TEXT.asParameter();
        else if (indexFeatures.supportsStringMapping(Mapping.TEXTSTRING)) return Mapping.TEXTSTRING.asParameter();
        throw new AssertionError("Text mapping not supported");
    }

    private Parameter getFieldMap(PropertyKey key) {
        return ParameterType.MAPPED_NAME.getParameter(key.name());
    }

    /**
     * See {@link JanusGraphIndexTest#testMultipleIndexBackends()}
     */
    @Test
    public void testBasic() {
        open(true, true);
        PropertyKey p1 = makeKey("p1", String.class);
        PropertyKey p2 = makeKey("p2", String.class);
        PropertyKey p3 = makeKey("p3", Long.class);
        mgmt.buildIndex("mixed", Vertex.class).addKey(p1, Mapping.STRING.asParameter()).buildMixedIndex(INDEX);
        mgmt.buildIndex("mi", Vertex.class).addKey(p2, Mapping.STRING.asParameter()).buildMixedIndex(INDEX2);
        mgmt.buildIndex("theIndex", Edge.class).addKey(p3).buildMixedIndex(INDEX);
        finishSchema();

        assertEquals(0, tx.traversal().V().has("p1", "val1").has("p2", "val2").count().next());
        Vertex v1 = tx.addVertex(T.id, getRandomId(), "p1", "val1", "p2", "val2");
        Vertex v2 = tx.addVertex(T.id, getRandomId(), "p1", "val1");
        Vertex v3 = tx.addVertex(T.id, getRandomId(), "p2", "val2");
        v1.addEdge("connects", v2, "p3", 1);
        v2.addEdge("connects", v3, "p3", 2);
        tx.commit();

        clopen(option(FORCE_INDEX_USAGE), true);
        assertEquals(2, tx.traversal().V().has("p1", "val1").count().next());
        assertEquals(2, tx.traversal().V().has("p2", "val2").count().next());
        assertEquals(1, tx.traversal().V().has("p1", "val1").has("p2", "val2").count().next());
        assertEquals(3, tx.traversal().V().or(__.has("p1", "val1"), __.has("p2", "val2")).count().next());
        assertEquals(3, tx.traversal().V().or(__.has("p1", "val1"), __.has("p2", "val2")).toList().size());
        assertEquals(1, tx.traversal().E().has("p3", 1).count().next());
        assertEquals(1, tx.traversal().E().has("p3", 1).toList().size());
    }

    /**
     * Modified from {@link JanusGraphIndexTest#testIndexUpdatesWithoutReindex()}
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test
    public void testIndexUpdatesWithoutReindex() throws InterruptedException, ExecutionException {
        open(true, true);
        final Object[] settings = new Object[]{option(LOG_SEND_DELAY, MANAGEMENT_LOG), Duration.ofMillis(0),
            option(KCVSLog.LOG_READ_LAG_TIME, MANAGEMENT_LOG), Duration.ofMillis(50),
            option(LOG_READ_INTERVAL, MANAGEMENT_LOG), Duration.ofMillis(250)
        };

        clopen(settings);

        //Creates types and index only two keys key
        mgmt.makePropertyKey("time").dataType(Integer.class).make();
        final PropertyKey text = mgmt.makePropertyKey("text").dataType(String.class).make();

        mgmt.makePropertyKey("height").dataType(Double.class).make();
        if (indexFeatures.supportsCardinality(Cardinality.LIST)) {
            mgmt.makePropertyKey("phone").dataType(String.class).cardinality(Cardinality.LIST).make();
        }
        mgmt.buildIndex("theIndex", Vertex.class).addKey(text, getTextMapping(), getFieldMap(text)).buildMixedIndex(INDEX);
        finishSchema();

        //Add initial data
        final String defText = "Mountain rocks are great friends";
        final int defTime = 5;
        final double defHeight = 101.1;
        final String[] defPhones = new String[]{"1234", "5678"};
        addVertex(defTime, defText, defHeight, defPhones);

        //Indexes should not yet be in use
        clopen(settings);
        evaluateQuery(tx.query().has("text", Text.CONTAINS, "rocks"),
            ElementCategory.VERTEX, 1, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().has("time", 5),
            ElementCategory.VERTEX, 1, new boolean[]{false, true});
        evaluateQuery(tx.query().interval("height", 100, 200),
            ElementCategory.VERTEX, 1, new boolean[]{false, true});
        evaluateQuery(tx.query().interval("height", 100, 200).has("time", 5),
            ElementCategory.VERTEX, 1, new boolean[]{false, true});
        evaluateQuery(tx.query().has("text", Text.CONTAINS, "rocks").has("time", 5).interval("height", 100, 200),
            ElementCategory.VERTEX, 1, new boolean[]{false, true}, "theIndex");
        if (indexFeatures.supportsCardinality(Cardinality.LIST)) {
            evaluateQuery(tx.query().has("phone", Cmp.EQUAL, "1234"),
                ElementCategory.VERTEX, 1, new boolean[]{false, true});
            evaluateQuery(tx.query().has("phone", Cmp.EQUAL, "5678"),
                ElementCategory.VERTEX, 1, new boolean[]{false, true});
        }
        newTx();

        //Add another key to index ------------------------------------------------------
        finishSchema();
        final PropertyKey time = mgmt.getPropertyKey("time");
        mgmt.addIndexKey(mgmt.getGraphIndex("theIndex"), time, getFieldMap(time));
        finishSchema();
        newTx();

        //Add more data
        addVertex(defTime, defText, defHeight, defPhones);
        tx.commit();
        //Should not yet be able to enable since not yet registered
        mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.ENABLE_INDEX);
        assertFalse(ManagementSystem.awaitGraphIndexStatus(graph, "theIndex")
            .status(SchemaStatus.ENABLED)
            .timeout(10L, ChronoUnit.SECONDS)
            .call()
            .getSucceeded());
        //This call is redundant and just here to make sure it doesn't mess anything up
        mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.REGISTER_INDEX).get();
        mgmt.commit();

        ManagementSystem.awaitGraphIndexStatus(graph, "theIndex").timeout(10L, ChronoUnit.SECONDS).call();

        finishSchema();
        mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.ENABLE_INDEX).get();
        finishSchema();

        //Add more data
        addVertex(defTime, defText, defHeight, defPhones);

        //One more key should be indexed but only sees partial data
        clopen(settings);
        evaluateQuery(tx.query().has("text", Text.CONTAINS, "rocks"),
            ElementCategory.VERTEX, 3, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().has("time", 5),
            ElementCategory.VERTEX, 2, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().interval("height", 100, 200),
            ElementCategory.VERTEX, 3, new boolean[]{false, true});
        evaluateQuery(tx.query().interval("height", 100, 200).has("time", 5),
            ElementCategory.VERTEX, 2, new boolean[]{false, true}, "theIndex");
        evaluateQuery(tx.query().has("text", Text.CONTAINS, "rocks").has("time", 5).interval("height", 100, 200),
            ElementCategory.VERTEX, 2, new boolean[]{false, true}, "theIndex");
        if (indexFeatures.supportsCardinality(Cardinality.LIST)) {
            evaluateQuery(tx.query().has("phone", Cmp.EQUAL, "1234"),
                ElementCategory.VERTEX, 3, new boolean[]{false, true});
            evaluateQuery(tx.query().has("phone", Cmp.EQUAL, "5678"),
                ElementCategory.VERTEX, 3, new boolean[]{false, true});
        }
        newTx();

        //Add another key to index ------------------------------------------------------
        finishSchema();
        final PropertyKey height = mgmt.getPropertyKey("height");
        mgmt.addIndexKey(mgmt.getGraphIndex("theIndex"), height);
        if (indexFeatures.supportsCardinality(Cardinality.LIST)) {
            final PropertyKey phone = mgmt.getPropertyKey("phone");
            mgmt.addIndexKey(mgmt.getGraphIndex("theIndex"), phone, new Parameter("mapping", Mapping.STRING));
        }
        finishSchema();

        //Add more data
        addVertex(defTime, defText, defHeight, defPhones);
        tx.commit();
        mgmt.commit();

        ManagementUtil.awaitGraphIndexUpdate(graph, "theIndex", 10, ChronoUnit.SECONDS);

        finishSchema();
        mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.ENABLE_INDEX);
        finishSchema();

        JanusGraphIndex index = mgmt.getGraphIndex("theIndex");
        for (final PropertyKey key : index.getFieldKeys()) {
            assertEquals(SchemaStatus.ENABLED, index.getIndexStatus(key));
        }

        //Add more data
        addVertex(defTime, defText, defHeight, defPhones);

        //One more key should be indexed but only sees partial data
        clopen(settings);
        evaluateQuery(tx.query().has("text", Text.CONTAINS, "rocks"),
            ElementCategory.VERTEX, 5, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().has("time", 5),
            ElementCategory.VERTEX, 4, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().interval("height", 100, 200),
            ElementCategory.VERTEX, 2, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().interval("height", 100, 200).has("time", 5),
            ElementCategory.VERTEX, 2, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().has("text", Text.CONTAINS, "rocks").has("time", 5).interval("height", 100, 200),
            ElementCategory.VERTEX, 2, new boolean[]{true, true}, "theIndex");
        if (indexFeatures.supportsCardinality(Cardinality.LIST)) {
            evaluateQuery(tx.query().has("phone", Cmp.EQUAL, "1234"),
                ElementCategory.VERTEX, 2, new boolean[]{true, true}, "theIndex");
            evaluateQuery(tx.query().has("phone", Cmp.EQUAL, "5678"),
                ElementCategory.VERTEX, 2, new boolean[]{true, true}, "theIndex");
        }
        newTx();
        finishSchema();

        mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.REINDEX).get();
        mgmt.commit();

        finishSchema();

        //All the data should now be in the index
        clopen(settings);
        evaluateQuery(tx.query().has("text", Text.CONTAINS, "rocks"),
            ElementCategory.VERTEX, 5, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().has("time", 5),
            ElementCategory.VERTEX, 5, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().interval("height", 100, 200),
            ElementCategory.VERTEX, 5, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().interval("height", 100, 200).has("time", 5),
            ElementCategory.VERTEX, 5, new boolean[]{true, true}, "theIndex");
        evaluateQuery(tx.query().has("text", Text.CONTAINS, "rocks").has("time", 5).interval("height", 100, 200),
            ElementCategory.VERTEX, 5, new boolean[]{true, true}, "theIndex");
        if (indexFeatures.supportsCardinality(Cardinality.LIST)) {
            evaluateQuery(tx.query().has("phone", Cmp.EQUAL, "1234"),
                ElementCategory.VERTEX, 5, new boolean[]{true, true}, "theIndex");
            evaluateQuery(tx.query().has("phone", Cmp.EQUAL, "5678"),
                ElementCategory.VERTEX, 5, new boolean[]{true, true}, "theIndex");
        }

        mgmt.updateIndex(mgmt.getGraphIndex("theIndex"), SchemaAction.DISABLE_INDEX).get();
        tx.commit();
        mgmt.commit();

        ManagementUtil.awaitGraphIndexUpdate(graph, "theIndex", 10, ChronoUnit.SECONDS);
        finishSchema();

        index = mgmt.getGraphIndex("theIndex");
        for (final PropertyKey key : index.getFieldKeys()) {
            assertEquals(SchemaStatus.DISABLED, index.getIndexStatus(key));
        }

        newTx();
        //This now requires a full graph scan
        evaluateQuery(tx.query().has("time", 5),
            ElementCategory.VERTEX, 5, new boolean[]{false, true});

    }
}
