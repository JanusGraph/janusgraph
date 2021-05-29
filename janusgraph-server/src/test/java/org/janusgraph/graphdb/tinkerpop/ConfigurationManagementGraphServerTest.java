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

package org.janusgraph.graphdb.tinkerpop;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConfigurationManagementGraphServerTest extends AbstractGremlinServerIntegrationTest {

    @Test
    public void ensureServerIsRunningCorrectly() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
    }

    @Test
    public void bindingForConfigurationManagementGraphShouldExists() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        assertEquals(0, client.submit("ConfigurationManagementGraph.vertices().size()").all().get().get(0).getInt());
    }

    @Test
    public void newGraphDoesntExistsBeforeCreation() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        //assert ConfigurationManagementGraph is bound
        assertEquals(0, client.submit("ConfigurationManagementGraph.vertices().size()").all().get().get(0).getInt());

        //assert new graph is not bound
        ExecutionException executionException = assertThrows(ExecutionException.class, () -> {

            client.submit("newGraph").all().get();
        });
        assertEquals(
            "org.apache.tinkerpop.gremlin.driver.exception.ResponseException: No such property: newGraph for class: Script3",
            executionException.getMessage());
    }

    @Test
    public void bindingShouldExistAfterGraphIsCreated() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        //create newGraph
        client.submit("Map<String, Object> map = new HashMap<String, Object>(); map.put(\"storage.backend\", \"inmemory\"); " +
            "org.janusgraph.core.ConfiguredGraphFactory.createTemplateConfiguration(" +
            "org.janusgraph.util.system.ConfigurationUtil.loadMapConfiguration(map));" +
            "org.janusgraph.core.ConfiguredGraphFactory.create(\"newGraph\")");

        Thread.sleep(1000,0);

        //assert newGraph is indeed bound
        assertEquals(0, client.submit("newGraph.vertices().size()").all().get().get(0).getInt());
        //assert newGraph_traversal is bound and the graphs are equivalent
        assertEquals("newGraph", client.submit("newGraph_traversal.getGraph().getGraphName()").all().get().get(0).getString());

        // Ensure that we can open a remote graph traversal source against the created graph, and execute traversals
        GraphTraversalSource newGraphTraversal = traversal().withRemote(DriverRemoteConnection.using(cluster, "newGraph_traversal"));
        assertEquals(0, newGraphTraversal.V().count().next().longValue());

        cluster.close();
    }
}

