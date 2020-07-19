// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.graphdb.server;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.util.DefaultGraphManager;
import org.javatuples.Pair;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JanusGraphManagerImplTest {

    private GraphManager getGraphManager() {
        Settings settings = new Settings();
        HashMap<String, String> map = new HashMap<>();
        map.put("graph", "src/test/resources/janusgraph-inmemory.properties");
        map.put("graph2", "src/test/resources/janusgraph-inmemory.properties");
        map.put("tinkergraph", "src/test/resources/tinkergraph.properties");
        settings.graphs = map;
        return new DefaultGraphManager(settings);
    }

    private Pair<Server, String> createServer(GraphManager graphManager) throws IOException {
        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder
            .forName(serverName).directExecutor().addService(new JanusGraphManagerImpl(graphManager)).build().start();
        return new Pair<>(server, serverName);
    }

    private Pair<TestingServerClosable, JanusGraphManagerGrpc.JanusGraphManagerBlockingStub> createServerStub(GraphManager graphManager) throws IOException {
        Pair<Server, String> server = createServer(graphManager);
        ManagedChannel channel = InProcessChannelBuilder.forName(server.getValue1()).directExecutor().build();
        JanusGraphManagerGrpc.JanusGraphManagerBlockingStub stub = JanusGraphManagerGrpc.newBlockingStub(channel);
        TestingServerClosable testingServerClosable = new TestingServerClosable(server.getValue0(), channel);
        return new Pair<>(testingServerClosable, stub);
    }

    @Test
    public void testContextIsReturnedForGivenGraphName() throws IOException {
        GraphManager graphManager = getGraphManager();
        Pair<TestingServerClosable, JanusGraphManagerGrpc.JanusGraphManagerBlockingStub> serverStub = createServerStub(graphManager);

        JanusGraphContext test = serverStub.getValue1().getJanusGraphContextByGraphName(
            GetJanusGraphContextByGraphNameRequest.newBuilder().setGraphName("graph").build());

        assertEquals("graph", test.getGraphName());
        serverStub.getValue0().close();
    }

    @Test
    public void testErrorIsReturnedForGivenGraphNameWhichIsNotAJanusGraph() throws IOException {
        GraphManager graphManager = getGraphManager();
        Pair<TestingServerClosable, JanusGraphManagerGrpc.JanusGraphManagerBlockingStub> serverStub = createServerStub(graphManager);

        assertThrows(StatusRuntimeException.class, () ->
            serverStub.getValue1().getJanusGraphContextByGraphName(
                GetJanusGraphContextByGraphNameRequest.newBuilder().setGraphName("tinkergraph").build()));
        serverStub.getValue0().close();
    }

    @Test
    public void testErrorIsReturnedForEmptyGivenGraphName() throws IOException {
        GraphManager graphManager = getGraphManager();
        Pair<TestingServerClosable, JanusGraphManagerGrpc.JanusGraphManagerBlockingStub> serverStub = createServerStub(graphManager);

        assertThrows(StatusRuntimeException.class, () ->
            serverStub.getValue1().getJanusGraphContextByGraphName(
                GetJanusGraphContextByGraphNameRequest.newBuilder().setGraphName("").build()));
        serverStub.getValue0().close();
    }

    @Test
    public void testErrorIsReturnedForUnknownGivenGraphName() throws IOException {
        GraphManager graphManager = getGraphManager();
        Pair<TestingServerClosable, JanusGraphManagerGrpc.JanusGraphManagerBlockingStub> serverStub = createServerStub(graphManager);

        assertThrows(StatusRuntimeException.class, () ->
            serverStub.getValue1().getJanusGraphContextByGraphName(
                GetJanusGraphContextByGraphNameRequest.newBuilder().setGraphName("test").build()));
        serverStub.getValue0().close();
    }

    @Test
    public void testErrorIsReturnedForNotSetGraphName() throws IOException {
        GraphManager graphManager = getGraphManager();
        Pair<TestingServerClosable, JanusGraphManagerGrpc.JanusGraphManagerBlockingStub> serverStub = createServerStub(graphManager);

        assertThrows(StatusRuntimeException.class, () ->
            serverStub.getValue1().getJanusGraphContextByGraphName(
                GetJanusGraphContextByGraphNameRequest.newBuilder().build()));
        serverStub.getValue0().close();
    }

    @Test
    public void testAllContextAreReturned() throws IOException {
        GraphManager graphManager = getGraphManager();
        Pair<TestingServerClosable, JanusGraphManagerGrpc.JanusGraphManagerBlockingStub> serverStub = createServerStub(graphManager);

        Iterator<JanusGraphContext> iterator = serverStub.getValue1().getJanusGraphContexts(
            GetJanusGraphContextsRequest.newBuilder().build());

        List<JanusGraphContext> actualList = new ArrayList<>();
        iterator.forEachRemaining(actualList::add);
        assertEquals(2, actualList.size());
        assertTrue(actualList.stream().anyMatch(it -> it.getGraphName().equals("graph")));
        assertTrue(actualList.stream().anyMatch(it -> it.getGraphName().equals("graph2")));
        serverStub.getValue0().close();
    }
}
