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
 * https://github.com/apache/tinkerpop/blob/3.2.7/gremlin-server/src/test/java/org/apache/tinkerpop/gremlin/server/TestClientFactory.java
 * and has been stripped down to the basic functionality needed here.
 *
 * Ideally, TinkerPop would include the src/test in its bundled jar, and
 * then we can directly import the class from the gremlin-server dependency.
 * We can follow those updates here:
 * https://issues.apache.org/jira/projects/TINKERPOP/issues/TINKERPOP-1900?filter=allopenissues
 */

package org.janusgraph.graphdb.tinkerpop;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.simple.WebSocketClient;

import java.net.URI;

/**
 * @author Stephen Mallette (https://stephen.genoprime.com)
 */
public final class TestClientFactory {

    public static final int PORT = 45940;
    public static final URI WEBSOCKET_URI = URI.create("ws://localhost:" + PORT + "/gremlin");
    public static final URI NIO_URI = URI.create("gs://localhost:" + PORT);
    public static final String HTTP = "http://localhost:" + PORT;

    public static Cluster.Builder build() {
        return Cluster.build("localhost").port(45940);
    }

    public static Cluster open() {
        return build().create();
    }

    public static WebSocketClient createWebSocketClient() {
        return new WebSocketClient(WEBSOCKET_URI);
    }

    public static String createURLString() {
        return createURLString("");
    }

    public static String createURLString(final String suffix) {
        return HTTP + suffix;
    }
}

