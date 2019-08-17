// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.graphdb.log;

import org.easymock.EasyMockSupport;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.log.Log;
import org.janusgraph.diskstorage.log.kcvs.KCVSLog;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;

import static org.easymock.EasyMock.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class StandardLogProcessorFrameworkTest  extends EasyMockSupport {

    @Test
    public void testAddLogProcessor() throws BackendException {
        StandardJanusGraph graph = createGraphWithMockedInternals();
        StandardLogProcessorFramework logProcessorFramework = new StandardLogProcessorFramework(graph);

        logProcessorFramework.addLogProcessor("foo-identifier")
            .addProcessor((tx, txId, changeState) -> {
                // no-op processor
            })
            .setStartTimeNow()
            .setProcessorIdentifier("bar-processor")
            .build();

        Map<String, Log> processorLogs = null;
        try {
            Field processorLogsField = logProcessorFramework.getClass().getDeclaredField("processorLogs");
            processorLogsField.setAccessible(true);
            processorLogs = (Map<String, Log>) processorLogsField.get(logProcessorFramework);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }

        assertNotNull(processorLogs, "processorLogs must not be null or empty after adding a log processor");
        assertEquals(processorLogs.isEmpty(), false, "processorLogs should be non-empty after adding a processor");
        verifyAll();
    }


    private StandardJanusGraph createGraphWithMockedInternals() throws BackendException {
        StandardJanusGraph mockGraph = createMock(StandardJanusGraph.class);
        Backend mockBackend = createMock(Backend.class);
        KCVSLog mockKCVSLog = createMock(KCVSLog.class);
        GraphDatabaseConfiguration gdbConfig = createMock(GraphDatabaseConfiguration.class);
        TimestampProvider tsProvider = createMock(TimestampProvider.class);
        Serializer mockSerializer = createMock(Serializer.class);

        expect(mockGraph.getConfiguration()).andReturn(gdbConfig);
        expect(mockGraph.isOpen()).andReturn(true).anyTimes();
        expect(mockGraph.getDataSerializer()).andReturn(mockSerializer);
        expect(mockGraph.getBackend()).andReturn(mockBackend);

        expect(mockBackend.getUserLog(anyString())).andReturn(mockKCVSLog);
        expect(gdbConfig.getTimestampProvider()).andReturn(tsProvider);

        mockKCVSLog.registerReaders(anyObject(), anyObject());


        replayAll();
        return mockGraph;
    }
}