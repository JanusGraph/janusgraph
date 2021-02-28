/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * This file was pulled from the Apache TinkerPop project:
 * https://github.com/apache/tinkerpop/blob/3.2.7/gremlin-server/src/test/java/org/apache/tinkerpop/gremlin/server/AbstractGremlinServerIntegrationTest.java
 * and has been stripped down to the basic functionality needed here.
 *
 * Ideally, TinkerPop would include the src/test in its bundled jar, and
 * then we can directly import the class from the gremlin-server dependency.
 * We can follow those updates here:
 * https://issues.apache.org/jira/projects/TINKERPOP/issues/TINKERPOP-1900?filter=allopenissues
 */

package org.janusgraph.graphdb.tinkerpop;

import org.apache.tinkerpop.gremlin.server.op.OpLoader;
import org.janusgraph.graphdb.management.ConfigurationManagementGraph;
import org.janusgraph.graphdb.management.JanusGraphManager;
import org.janusgraph.graphdb.server.JanusGraphServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts and stops an instance for each executed test.
 */
public abstract class AbstractGremlinServerIntegrationTest {
    protected JanusGraphServer server;
    private static final Logger logger = LoggerFactory.getLogger(AbstractGremlinServerIntegrationTest.class);

    public String getSettingsPath() {
        return "src/test/resources/gremlin-server-integration.yaml";
    }

    @BeforeEach
    public void setUp(TestInfo testInfo) {
        logger.info("* Testing: " + testInfo.getDisplayName());
        startServer();
    }

    public void startServer() {
        this.server = new JanusGraphServer(getSettingsPath());
        server.start().join();
    }

    @AfterEach
    public void tearDown() {
        stopServer();
        JanusGraphManager.resetInstance();
        ConfigurationManagementGraph.shutdownConfigurationManagementGraph();
    }

    public void stopServer() {
        server.stop().join();
        // reset the OpLoader processors so that they can get reconfigured on startup - Settings may have changed
        // between tests
        OpLoader.reset();
    }
}

