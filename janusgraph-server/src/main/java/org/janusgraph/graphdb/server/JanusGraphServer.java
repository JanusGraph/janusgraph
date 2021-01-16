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
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.janusgraph.graphdb.server.utils.GremlinSettingsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class JanusGraphServer {
    private static final Logger logger = LoggerFactory.getLogger(JanusGraphServer.class);
    private GremlinServer gremlinServer = null;
    private Settings gremlinSettings = null;
    private final String confPath;
    private CompletableFuture<Void> serverStarted = null;
    private CompletableFuture<Void> serverStopped = null;

    public JanusGraphServer(final String file) {
        confPath = file;
    }

    public static void main(final String[] args) throws Exception {
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

    public synchronized CompletableFuture<Void> start() {
        if (serverStarted != null) {
            return serverStarted;
        }
        serverStarted = new CompletableFuture<>();
        try
        {
            logger.info("Configuring JanusGraph Server from {}", confPath);
            gremlinSettings = GremlinSettingsUtils.configureDefaultSerializersIfNotSet(Settings.read(confPath));
            gremlinServer = new GremlinServer(gremlinSettings);
            serverStarted = CompletableFuture.allOf(gremlinServer.start());
        }
        catch(Exception ex){
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
        logger.info("JanusGraph Version: {}", Manifests.read("janusgraphVersion"));
        logger.info("TinkerPop Version: {}", Manifests.read("tinkerpopVersion"));
    }
}
