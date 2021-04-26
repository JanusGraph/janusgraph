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

package org.janusgraph.graphdb.hbase;

import org.janusgraph.HBaseContainer;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ASSIGN_TIMESTAMP;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Testcontainers
public class HBaseGraphTest extends JanusGraphTest {
    @Container
    public static final HBaseContainer hBaseContainer = new HBaseContainer();

    @Override
    public WriteConfiguration getConfiguration() {
        return hBaseContainer.getWriteConfiguration();
    }

    @Override @Test @Disabled("HBase does not support retrieving cell TTL by client")
    public void testVertexTTLImplicitKey() { }

    @Override @Test @Disabled("HBase does not support retrieving cell TTL by client")
    public void testEdgeTTLImplicitKey() { }

    protected static Stream<Arguments> generateConsistencyConfigs() {
        return Arrays.stream(new Arguments[]{
            arguments(true),
            arguments(false)
        });
    }

    @Override
    @Test
    @Disabled
    public void testConsistencyEnforcement() {
        // disable original test in favour of parameterized test
    }

    @ParameterizedTest
    @MethodSource("generateConsistencyConfigs")
    public void testConsistencyEnforcement(boolean assignTimestamp) {
        clopen(option(ASSIGN_TIMESTAMP), assignTimestamp);
        super.testConsistencyEnforcement();
    }

    @Override
    @Test
    @Disabled
    public void testConcurrentConsistencyEnforcement() {
        // disable original test in favour of parameterized test
    }

    @ParameterizedTest
    @MethodSource("generateConsistencyConfigs")
    public void testConcurrentConsistencyEnforcement(boolean assignTimestamp) throws Exception {
        clopen(option(ASSIGN_TIMESTAMP), assignTimestamp);
        super.testConcurrentConsistencyEnforcement();
    }
}
