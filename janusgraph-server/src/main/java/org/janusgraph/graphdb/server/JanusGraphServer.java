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

import com.jcabi.manifests.Manifests;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.janusgraph.graphdb.grpc.JanusGraphContextHandler;
import org.janusgraph.graphdb.grpc.JanusGraphManagerServiceImpl;
import org.janusgraph.graphdb.grpc.schema.SchemaManagerImpl;
import org.janusgraph.graphdb.management.JanusGraphManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class JanusGraphServer {
    private static final Logger logger = LoggerFactory.getLogger(JanusGraphServer.class);
    private GremlinServer gremlinServer = null;
    private JanusGraphSettings janusGraphSettings = null;
    private final String confPath;
    private CompletableFuture<Void> serverStarted = null;
    private CompletableFuture<Void> serverStopped = null;
    private Server grpcServer = null;

    public static final String MANIFEST_JANUSGRAPH_VERSION_ATTRIBUTE = "janusgraphVersion";
    public static final String MANIFEST_TINKERPOP_VERSION_ATTRIBUTE = "tinkerpopVersion";

    public JanusGraphServer(final String file) {
        confPath = file;
    }

    public static void main(final String[] args) {
        printHeader();
        final String file = (args.length > 0) ? args[0] : "conf/janusgraph-server.yaml";
        JanusGraphServer janusGraphServer = new JanusGraphServer(file);
        janusGraphServer.start().exceptionally(t -> {
            logger.error("JanusGraph Server was unable to start and will now begin shutdown", t);
            janusGraphServer.stop().join();
            return null;
        }).join();
    }

    public GremlinServer getGremlinServer() {
        return gremlinServer;
    }

    public JanusGraphSettings getJanusGraphSettings() {
        return janusGraphSettings;
    }

    private Server createGrpcServer(JanusGraphSettings janusGraphSettings, GraphManager graphManager) {
        JanusGraphContextHandler janusGraphContextHandler = new JanusGraphContextHandler(graphManager);
        return ServerBuilder
            .forPort(janusGraphSettings.getGrpcServer().getPort())
            .addService(new JanusGraphManagerServiceImpl(janusGraphContextHandler))
            .addService(new SchemaManagerImpl(janusGraphContextHandler))
            .build();
    }

    public synchronized CompletableFuture<Void> start() {
        if (serverStarted != null) {
            return serverStarted;
        }
        serverStarted = new CompletableFuture<>();
        try {
            logger.info("Configuring JanusGraph Server from {}", confPath);
            janusGraphSettings = JanusGraphSettings.read(confPath);
            gremlinServer = new GremlinServer(janusGraphSettings);
            CompletableFuture<Void> grpcServerFuture = CompletableFuture.completedFuture(null);
            if (janusGraphSettings.getGrpcServer().isEnabled()) {
                grpcServerFuture = CompletableFuture.runAsync(() -> {
                    GraphManager graphManager = gremlinServer.getServerGremlinExecutor().getGraphManager();
                    grpcServer = createGrpcServer(janusGraphSettings, graphManager);
                    try {
                        grpcServer.start();
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                });
            }
            CompletableFuture<Void> gremlinServerFuture = gremlinServer.start()
                .thenAcceptAsync(JanusGraphServer::configure);
            serverStarted = CompletableFuture.allOf(gremlinServerFuture, grpcServerFuture);
        } catch (Exception ex) {
            serverStarted.completeExceptionally(ex);
        }
        return serverStarted;
    }

    private static void configure(ServerGremlinExecutor serverGremlinExecutor) {
        GraphManager graphManager = serverGremlinExecutor.getGraphManager();
        if (!(graphManager instanceof JanusGraphManager)){
            return;
        }
        ((JanusGraphManager) graphManager).configureGremlinExecutor(serverGremlinExecutor.getGremlinExecutor());
    }

    public synchronized CompletableFuture<Void> stop() {
        if (gremlinServer == null) {
            return CompletableFuture.completedFuture(null);
        }
        if (serverStopped != null) {
            return serverStopped;
        }
        if (grpcServer != null) {
            grpcServer.shutdownNow();
        }
        serverStopped = gremlinServer.stop();
        return serverStopped;
    }

    private static String getHeader() {
        return "                                                                      " + System.lineSeparator() +
            "   mmm                                mmm                       #     " + System.lineSeparator() +
            "     #   mmm   m mm   m   m   mmm   m\"   \"  m mm   mmm   mmmm   # mm  " + System.lineSeparator() +
            "     #  \"   #  #\"  #  #   #  #   \"  #   mm  #\"  \" \"   #  #\" \"#  #\"  # " + System.lineSeparator() +
            "     #  m\"\"\"#  #   #  #   #   \"\"\"m  #    #  #     m\"\"\"#  #   #  #   # " + System.lineSeparator() +
            " \"mmm\"  \"mm\"#  #   #  \"mm\"#  \"mmm\"   \"mmm\"  #     \"mm\"#  ##m#\"  #   # " + System.lineSeparator() +
            "                                                         #            " + System.lineSeparator() +
            "                                                         \"            " + System.lineSeparator();
    }

    private static void printHeader() {
        logger.info(getHeader());
        logger.info("JanusGraph Version: {}", Manifests.read(MANIFEST_JANUSGRAPH_VERSION_ATTRIBUTE));
        logger.info("TinkerPop Version: {}", Manifests.read(MANIFEST_TINKERPOP_VERSION_ATTRIBUTE));
    }
}
