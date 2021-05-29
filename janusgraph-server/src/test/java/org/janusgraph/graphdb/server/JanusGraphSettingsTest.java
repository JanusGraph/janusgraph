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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JanusGraphSettingsTest {

    @Test
    public void testGrpcServerDefaultValues() throws Exception {
        JanusGraphSettings settings = JanusGraphSettings.read("src/test/resources/janusgraph-server-without-serializers.yaml");

        assertFalse(settings.getGrpcServer().isEnabled());
        assertEquals(10182, settings.getGrpcServer().getPort());
    }

    @Test
    public void testGrpcServerOverwriteDefaultValues() throws Exception {
        JanusGraphSettings settings = JanusGraphSettings.read("src/test/resources/janusgraph-server-with-grpc.yaml");

        assertTrue(settings.getGrpcServer().isEnabled());
        assertEquals(8042, settings.getGrpcServer().getPort());
    }

    @Test
    public void testLoadGremlinServerCorrectly() throws Exception {
        JanusGraphSettings janusGraphSettings = JanusGraphSettings.read("src/test/resources/janusgraph-server-with-grpc.yaml");

        assertEquals("0.0.0.0", janusGraphSettings.host);
        assertEquals(1, janusGraphSettings.graphs.size());
        assertEquals(11, janusGraphSettings.serializers.size());
    }
}
