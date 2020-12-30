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

package org.janusgraph.pkgtest;

import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.driver.ser.AbstractMessageSerializer.TOKEN_IO_REGISTRIES;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTimeout;

public abstract class AbstractJanusGraphAssemblyIT extends JanusGraphAssemblyBaseIT {

    protected abstract String getConfigPath();

    protected abstract String getServerConfigPath();

    protected abstract String getGraphName();

    @Test
    public void testSingleVertexInteractionAgainstGremlinSh() throws Exception {
        unzipAndRunExpect("single-vertex.expect.vm", getConfigPath(), getGraphName(), false, false);
    }

    @Test
    @Tag(TestCategory.FULL_TESTS)
    public void testSingleVertexInteractionAgainstGremlinShFull() throws Exception {
        unzipAndRunExpect("single-vertex.expect.vm", getConfigPath(), getGraphName(), true, false);
    }

    @Test
    public void testGettingStartedAgainstGremlinSh() throws Exception {
        unzipAndRunExpect("getting-started.expect.vm", getConfigPath(), getGraphName(), false, false);
    }

    @Test
    @Tag(TestCategory.FULL_TESTS)
    public void testGettingStartedAgainstGremlinShFull() throws Exception {
        unzipAndRunExpect("getting-started.expect.vm", getConfigPath(), getGraphName(), true, false);
    }

    @Test
    public void testJanusGraphServerGremlin() throws Exception {
        testJanusGraphServer(false);
    }

    @Test
    @Tag(TestCategory.FULL_TESTS)
    public void testJanusGraphServerGremlinFull() throws Exception {
        testJanusGraphServer(true);
    }

    protected MessageSerializer createGryoMessageSerializer() {
        return new GryoMessageSerializerV3d0(GryoMapper.build().addRegistry(JanusGraphIoRegistry.instance()));
    }

    protected MessageSerializer createGraphSONMessageSerializer() {
        return new GraphSONMessageSerializerV3d0(GraphSONMapper.build().addRegistry(JanusGraphIoRegistry.instance()));
    }

    protected MessageSerializer createGraphBinaryMessageSerializerV1() {
        final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1();
        final Map<String, Object> config = new HashMap<>();
        config.put(TOKEN_IO_REGISTRIES, Collections.singletonList(JanusGraphIoRegistry.class.getName()));
        serializer.configure(config, Collections.emptyMap());
        return serializer;
    }

    protected static boolean serverListening(String host, int port) {
        Socket s = null;
        try {
            s = new Socket(host, port);
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            if (s != null)
                try {
                    s.close();
                } catch (Exception e) {
                }
        }
    }

    private void testServerUsingTraversalSource(GraphTraversalSource g) {
        g.addV("Test").iterate();
        List<Vertex> vertices = g.V().hasLabel("Test").toList();
        assertNotEquals(0, vertices.size());
    }

    protected void runTraversalAgainstServer(MessageSerializer serializer) {
        Cluster cluster = Cluster.build("localhost")
            .port(8182)
            .serializer(serializer)
            .create();

        GraphTraversalSource g = AnonymousTraversalSource.traversal()
            .withRemote(DriverRemoteConnection.using(cluster, "g"));

        testServerUsingTraversalSource(g);
    }

    protected void testJanusGraphServer(boolean full) throws Exception {
        final boolean debug = false;
        ImmutableMap<String, String> contextVars = ImmutableMap.of("janusgraphServerConfig", getServerConfigPath());
        unzipAndRunExpect("janusgraph-server-sh.before.expect.vm", contextVars, full, debug);
        assertTimeout(Duration.ofSeconds(30), () -> {
            while (!serverListening("localhost", 8182)) {
                Thread.sleep(1000);
            }
        });

        runTraversalAgainstServer(createGraphSONMessageSerializer());
        runTraversalAgainstServer(createGraphBinaryMessageSerializerV1());
        runTraversalAgainstServer(createGryoMessageSerializer());

        parseTemplateAndRunExpect("janusgraph-server-sh.after.expect.vm", contextVars, full, debug);
    }
}
