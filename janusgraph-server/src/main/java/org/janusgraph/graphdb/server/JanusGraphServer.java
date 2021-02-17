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
import org.apache.tinkerpop.gremlin.server.Settings;
import org.janusgraph.graphdb.grpc.JanusGraphManagerServiceImpl;
import org.janusgraph.graphdb.server.utils.GremlinSettingsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class JanusGraphServer {
    private static final Logger logger = LoggerFactory.getLogger(JanusGraphServer.class);
    private GremlinServer gremlinServer = null;
    private Settings gremlinSettings = null;
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
            logger.error("JanusGraph Server was unable to start and will now begin shutdown: {}", t.getMessage());
            janusGraphServer.stop().join();
            return null;
        }).join();
    }

    public GremlinServer getGremlinServer() {
        return gremlinServer;
    }

    public Settings getGremlinSettings() {
        return gremlinSettings;
    }

    public JanusGraphSettings getJanusGraphSettings() {
        return janusGraphSettings;
    }

    private Server createGrpcServer(JanusGraphSettings janusGraphSettings, GraphManager graphManager) {
        return ServerBuilder
            .forPort(janusGraphSettings.getGrpcServer().getPort())
            .addService(new JanusGraphManagerServiceImpl(graphManager))
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
            gremlinSettings = GremlinSettingsUtils.configureDefaultSerializersIfNotSet(janusGraphSettings.getGremlinSettings());
            gremlinServer = new GremlinServer(gremlinSettings);
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
            serverStarted = CompletableFuture.allOf(gremlinServer.start(), grpcServerFuture);
        } catch (Exception ex) {
            serverStarted.completeExceptionally(ex);
        }
        return serverStarted;
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
