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
import org.janusgraph.graphdb.server.utils.JanusGraphSettingsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JanusGraphServer {
    private static final Logger logger = LoggerFactory.getLogger(JanusGraphServer.class);

    public static void main(final String[] args) throws Exception {
        printHeader();
        final String file;
        if (args.length > 0)
            file = args[0];
        else
            file = "conf/janusgraph-server.yaml";

        final Settings settings;
        try {
            settings = JanusGraphSettingsUtils.configureDefaultSerializersIfNotSet(Settings.read(file));
        } catch (Exception ex) {
            logger.error("Configuration file at {} could not be found or parsed properly. [{}]", file, ex.getMessage());
            return;
        }

        logger.info("Configuring JanusGraph Server from {}", file);
        final GremlinServer server = new GremlinServer(settings);
        server.start().exceptionally(t -> {
            logger.error("JanusGraph Server was unable to start and will now begin shutdown: {}", t.getMessage());
            server.stop().join();
            return null;
        }).join();
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
        logger.info("JanusGraph Version: " +  Manifests.read("janusgraphVersion"));
        logger.info("TinkerPop Version: " +  Manifests.read("tinkerpopVersion"));
    }
}
