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

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.util.DefaultGraphManager;
import org.janusgraph.graphdb.server.TestingServerClosable;
import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.HashMap;

public abstract class JanusGraphGrpcServerBaseTest {

    protected GraphManager graphManager;
    protected ManagedChannel managedChannel;
    private TestingServerClosable closable;
    protected JanusGraphManagerServiceGrpc.JanusGraphManagerServiceBlockingStub blockingStub;

    protected GraphManager getGraphManager() {
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
            .forName(serverName).directExecutor()
            .addService(new JanusGraphManagerServiceImpl(graphManager)).build().start();
        return new Pair<>(server, serverName);
    }

    @BeforeEach
    public void startServer() throws IOException {
        graphManager = getGraphManager();
        Pair<Server, String> server = createServer(graphManager);
        managedChannel = InProcessChannelBuilder.forName(server.getValue1()).directExecutor().build();
        closable = new TestingServerClosable(server.getValue0(), managedChannel);
        blockingStub = JanusGraphManagerServiceGrpc.newBlockingStub(managedChannel);
    }

    @AfterEach
    public void stopServer() throws IOException {
        closable.close();
    }
}
