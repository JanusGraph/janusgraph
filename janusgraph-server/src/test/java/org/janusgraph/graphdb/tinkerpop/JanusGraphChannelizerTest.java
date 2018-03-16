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
import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.equalTo;

public class JanusGraphChannelizerTest extends AbstractGremlinServerIntegrationTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void bindingShouldExistAfterGraphIsCreated() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            //assert server is running correctly
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());

            //assert ConfigurationManagementGraph is bound
            assertEquals(0, client.submit("ConfigurationManagementGraph.vertices().size()").all().get().get(0).getInt());

            //assert new graph is not bound
            thrown.expect(ExecutionException.class);
            thrown.expectMessage(equalTo("org.apache.tinkerpop.gremlin.driver.exception.ResponseException: No such property: " +
                "newGraph for class: Script3"));
            client.submit("newGraph").all().get();

            //create newGraph
            client.submit("Map<String, Object> map = new HashMap<String, Object>(); map.put(\"storage.backend\", \"inmemory\"); " +
                "org.janusgraph.core.ConfiguredGraphFactory.createTemplateConfiguration(new MapConfiguration(map));" +
                "org.janusgraph.core.ConfiguredGraphFactory.create(\"newGraph\")");
            //assert newGraph is indeed bound
            assertEquals(0, client.submit("newGraph.vertices().size()").all().get().get(0).getInt());
            //assert newGraph_traversal is bound and the graphs are equivalent
            assertEquals("newGraph", client.submit("newGraph_traversal.getGraph().getGraphName()").all().get().get(0).getString());

        } finally {
            cluster.close();
        }
    }
}

