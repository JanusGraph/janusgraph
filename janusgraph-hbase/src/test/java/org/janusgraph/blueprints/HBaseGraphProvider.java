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

package org.janusgraph.blueprints;

import org.janusgraph.HBaseContainer;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class HBaseGraphProvider extends AbstractJanusGraphProvider {
    private static final Logger logger = LoggerFactory.getLogger(HBaseGraphProvider.class);
    public static final HBaseContainer HBASE_CONTAINER;

    static {
        waitForBindablePort(2181);
        waitForBindablePort(16000);
        HBASE_CONTAINER = new HBaseContainer();
        HBASE_CONTAINER.start();
    }

    private static void waitForBindablePort(int port) {
        boolean canBindPort = false;
        do {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                assertNotNull(serverSocket);
                assertEquals(serverSocket.getLocalPort(), port);
                serverSocket.close();
                canBindPort = true;
                continue;
            } catch (IOException e) {
                logger.warn("can't bind port", e);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.warn("can't sleep", e);
            }
        } while (!canBindPort);
    }

    @Override
    public ModifiableConfiguration getJanusGraphConfiguration(String graphName, Class<?> test, String testMethodName) {
        return HBASE_CONTAINER.getNamedConfiguration(graphName, "");
    }
}
