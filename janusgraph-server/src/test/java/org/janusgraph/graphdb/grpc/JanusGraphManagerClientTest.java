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

package org.janusgraph.graphdb.grpc;

import io.grpc.StatusRuntimeException;
import org.janusgraph.graphdb.grpc.types.JanusGraphContext;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JanusGraphManagerClientTest extends JanusGraphGrpcServerBaseTest {

    @Test
    public void testGetJanusGraphVersion() {
        JanusGraphManagerClient janusGraphManagerClient = new JanusGraphManagerClient(managedChannel);

        String version = janusGraphManagerClient.getJanusGraphVersion();

        assertNotEquals("", version);
    }

    @Test
    public void testGetTinkerPopVersion() {
        JanusGraphManagerClient janusGraphManagerClient = new JanusGraphManagerClient(managedChannel);

        String version = janusGraphManagerClient.getTinkerPopVersion();

        assertNotEquals("", version);
    }

    @Test
    public void testContextIsReturnedForGivenGraphName() {
        JanusGraphManagerClient janusGraphManagerClient = new JanusGraphManagerClient(managedChannel);

        JanusGraphContext context = janusGraphManagerClient.getContextByGraphName("graph");

        assertEquals("graph", context.getGraphName());
    }

    @Test
    public void testErrorIsReturnedForGivenGraphNameWhichIsNotAJanusGraph() {
        JanusGraphManagerClient janusGraphManagerClient = new JanusGraphManagerClient(managedChannel);

        assertThrows(StatusRuntimeException.class, () ->
            janusGraphManagerClient.getContextByGraphName("tinkergraph"));
    }

    @Test
    public void testErrorIsReturnedForEmptyGivenGraphName() {
        JanusGraphManagerClient janusGraphManagerClient = new JanusGraphManagerClient(managedChannel);

        assertThrows(StatusRuntimeException.class, () ->
            janusGraphManagerClient.getContextByGraphName(""));
    }

    @Test
    public void testErrorIsReturnedForUnknownGivenGraphName() {
        JanusGraphManagerClient janusGraphManagerClient = new JanusGraphManagerClient(managedChannel);

        assertThrows(StatusRuntimeException.class, () ->
            janusGraphManagerClient.getContextByGraphName("test"));
    }

    @Test
    public void testAllContextAreReturned() {
        JanusGraphManagerClient janusGraphManagerClient = new JanusGraphManagerClient(managedChannel);

        List<JanusGraphContext> contexts = janusGraphManagerClient.getContexts();

        assertEquals(2, contexts.size());
        assertTrue(contexts.stream().anyMatch(it -> it.getGraphName().equals("graph")));
        assertTrue(contexts.stream().anyMatch(it -> it.getGraphName().equals("graph2")));
    }
}
