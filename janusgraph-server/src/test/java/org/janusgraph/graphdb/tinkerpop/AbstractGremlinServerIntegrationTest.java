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

import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.OpLoader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;

/**
 * Starts and stops an instance for each executed test.
 */
public abstract class AbstractGremlinServerIntegrationTest {
    protected GremlinServer server;
    private final static String epollOption = "gremlin.server.epoll";
    private static final boolean GREMLIN_SERVER_EPOLL = "true".equalsIgnoreCase(System.getProperty(epollOption));
    private static final Logger logger = LoggerFactory.getLogger(AbstractGremlinServerIntegrationTest.class);

    public Settings overrideSettings(final Settings settings) {
        return settings;
    }

    public InputStream getSettingsInputStream() {
        return AbstractGremlinServerIntegrationTest.class.getResourceAsStream("gremlin-server-integration.yaml");
    }

    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        logger.info("* Testing: " + testInfo.getDisplayName());
        logger.info("* Epoll option enabled:" + GREMLIN_SERVER_EPOLL);

        startServer();
    }

    public void setUp(final Settings settings, TestInfo testInfo) throws Exception {
        logger.info("* Testing: " + testInfo.getDisplayName());
        logger.info("* Epoll option enabled:" + GREMLIN_SERVER_EPOLL);

        startServer(settings);
    }

    public void startServer(final Settings settings) throws Exception {
        if (null == settings) {
            startServer();
        } else {
            final Settings overriddenSettings = overrideSettings(settings);
            if (GREMLIN_SERVER_EPOLL) {
                overriddenSettings.useEpollEventLoop = true;
            }
            this.server = new GremlinServer(overriddenSettings);
            server.start().join();

        }
    }

    public void startServer() throws Exception {
        final InputStream stream = getSettingsInputStream();
        final Settings settings = Settings.read(stream);
        final Settings overriddenSettings = overrideSettings(settings);
        if (GREMLIN_SERVER_EPOLL) {
            overriddenSettings.useEpollEventLoop = true;
        }

        this.server = new GremlinServer(overriddenSettings);

        server.start().join();
    }

    @AfterEach
    public void tearDown() throws Exception {
        stopServer();
    }

    public void stopServer() throws Exception {
        server.stop().join();
        // reset the OpLoader processors so that they can get reconfigured on startup - Settings may have changed
        // between tests
        OpLoader.reset();
    }

    public static boolean deleteDirectory(final File directory) {
        if (directory.exists()) {
            final File[] files = directory.listFiles();
            if (null != files) {
                for (int i = 0; i < files.length; i++) {
                    if (files[i].isDirectory()) {
                        deleteDirectory(files[i]);
                    } else {
                        files[i].delete();
                    }
                }
            }
        }

        return (directory.delete());
    }
}

