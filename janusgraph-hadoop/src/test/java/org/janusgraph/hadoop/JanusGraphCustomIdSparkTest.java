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


package org.janusgraph.hadoop;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphBaseTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.testutil.TestGraphConfigs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ALLOW_SETTING_VERTEX_ID;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ALLOW_STRING_VERTEX_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class JanusGraphCustomIdSparkTest extends JanusGraphBaseTest {

    private static final Logger log = LoggerFactory.getLogger(JanusGraphCustomIdSparkTest.class);

    @Override
    public WriteConfiguration getConfiguration() {
        return getModifiableConfiguration().getConfiguration();
    }

    protected abstract ModifiableConfiguration getModifiableConfiguration();

    protected abstract Graph getSparkGraph() throws IOException, ConfigurationException;

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

    private void open(boolean allowSettingVertexId, boolean allowStringVertexId) {
        ModifiableConfiguration config = getModifiableConfiguration();
        config.set(ALLOW_SETTING_VERTEX_ID, allowSettingVertexId, new String[0]);
        config.set(ALLOW_STRING_VERTEX_ID, allowStringVertexId, new String[0]);
        open(config.getConfiguration());
    }

    /**
     * See {@link AbstractInputFormatIT#testReadWideVertexWithManyProperties()}
     * @throws Exception
     */
    @Test
    public void testReadWideVertexWithManyProperties() throws Exception {
        open(true, true);
        int numProps = 1 << 16;

        long numV  = 1;
        mgmt.makePropertyKey("p").cardinality(Cardinality.LIST).dataType(Integer.class).make();
        mgmt.commit();
        finishSchema();

        for (int j = 0; j < numV; j++) {
            Vertex v = graph.addVertex(T.id, UUID.randomUUID().toString().replace('-', ':'));
            for (int i = 0; i < numProps; i++) {
                v.property("p", i);
                if (i % 8 == 0) {
                    graph.tx().commit();
                }
            }
        }
        graph.tx().commit();

        assertEquals(numV, (long) graph.traversal().V().count().next());
        Map<Object, Object> propertiesOnVertex = graph.traversal().V().valueMap().next();
        List<?> valuesOnP = (List)propertiesOnVertex.values().iterator().next();
        assertEquals(numProps, valuesOnP.size());
        for (int i = 0; i < numProps; i++) {
            // ensure properties are read in the same order as they are inserted
            assertEquals(Integer.toString(i), valuesOnP.get(i).toString());
        }

        GraphTraversalSource t = getSparkGraph().traversal().withComputer(SparkGraphComputer.class);
        assertEquals(numV, (long) t.V().count().next());
        propertiesOnVertex = t.V().valueMap().next();
        final Set<?> observedValuesOnP = Collections.unmodifiableSet(new HashSet<>((List)propertiesOnVertex.values().iterator().next()));
        assertEquals(numProps, observedValuesOnP.size());
        // order may not be preserved in multi-value properties
        assertEquals(Collections.unmodifiableSet(new HashSet<>(valuesOnP)), observedValuesOnP, "Unexpected values");
    }

}
