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

package org.janusgraph.graphdb.grpc;

import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class JanusGraphManagerImplTest extends JanusGraphGrpcServerBaseTest {

    @Test
    public void testContextIsReturnedForGivenGraphName() {
        GetJanusGraphContextByGraphNameResponse response = blockingStub.getJanusGraphContextByGraphName(
            GetJanusGraphContextByGraphNameRequest.newBuilder().setGraphName("graph").build());

        assertEquals("graph", response.getContext().getGraphName());
    }

    @Test
    public void testErrorIsReturnedForGivenGraphNameWhichIsNotAJanusGraph() {
        assertThrows(StatusRuntimeException.class, () ->
            blockingStub.getJanusGraphContextByGraphName(
                GetJanusGraphContextByGraphNameRequest.newBuilder().setGraphName("tinkergraph").build()));
    }

    @Test
    public void testErrorIsReturnedForEmptyGivenGraphName() {
        assertThrows(StatusRuntimeException.class, () ->
            blockingStub.getJanusGraphContextByGraphName(
                GetJanusGraphContextByGraphNameRequest.newBuilder().setGraphName("").build()));
    }

    @Test
    public void testErrorIsReturnedForUnknownGivenGraphName() {
        assertThrows(StatusRuntimeException.class, () ->
            blockingStub.getJanusGraphContextByGraphName(
                GetJanusGraphContextByGraphNameRequest.newBuilder().setGraphName("test").build()));
    }

    @Test
    public void testErrorIsReturnedForNotSetGraphName() {
        assertThrows(StatusRuntimeException.class, () ->
            blockingStub.getJanusGraphContextByGraphName(
                GetJanusGraphContextByGraphNameRequest.newBuilder().build()));
    }

    @Test
    public void testAllContextAreReturned() {
        GetJanusGraphContextsResponse response = blockingStub.getJanusGraphContexts(
            GetJanusGraphContextsRequest.newBuilder().build());

        List<JanusGraphContext> actualList = response.getContextsList();
        assertEquals(2, actualList.size());
        assertTrue(actualList.stream().anyMatch(it -> it.getGraphName().equals("graph")));
        assertTrue(actualList.stream().anyMatch(it -> it.getGraphName().equals("graph2")));
    }
}
