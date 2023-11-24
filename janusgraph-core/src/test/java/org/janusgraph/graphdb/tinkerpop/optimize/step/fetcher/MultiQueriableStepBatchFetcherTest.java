// Copyright 2023 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.optimize.step.fetcher;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphMultiVertexQuery;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.anyCollection;

public class MultiQueriableStepBatchFetcherTest {

    private Vertex mockVertex;

    Traversal.Admin<?, ?> traversal;

    @BeforeEach
    public void setupMocks(){
        JanusGraphMultiVertexQuery multiQuery = Mockito.mock(JanusGraphMultiVertexQuery.class);
        JanusGraphTransaction tx = Mockito.mock(JanusGraphTransaction.class);
        mockVertex = Mockito.mock(JanusGraphVertex.class);
        traversal = Mockito.mock(Traversal.Admin.class);
        Mockito.doReturn(EmptyStep.instance()).when(traversal).getParent();
        Mockito.doReturn(traversal).when(traversal).asAdmin();
        Mockito.doReturn(Optional.of(tx)).when(traversal).getGraph();
        Mockito.doReturn(true).when(tx).isOpen();
        Mockito.doReturn(multiQuery).when(tx).multiQuery(anyCollection());
    }

    @Test
    public void testTraversalInterruptedExceptionIsThrownDuringInterruptedMultiQuery(){
        JanusGraphException causeException = new JanusGraphException(new InterruptedException());
        MultiQueriableStepBatchErrorFetcher fetcher = new MultiQueriableStepBatchErrorFetcher(causeException);
        TraversalInterruptedException resultingException = Assertions.assertThrows(TraversalInterruptedException.class, () -> fetcher.fetchData(traversal, mockVertex, 0));
        Assertions.assertEquals(causeException, resultingException.getCause());
    }

    @Test
    public void testJanusGraphExceptionIsNotLostDuringMultiQuery(){
        JanusGraphException causeException = new JanusGraphException(new RuntimeException());
        MultiQueriableStepBatchErrorFetcher fetcher = new MultiQueriableStepBatchErrorFetcher(causeException);
        JanusGraphException resultingException = Assertions.assertThrows(JanusGraphException.class, () -> fetcher.fetchData(traversal, mockVertex, 0));
        Assertions.assertEquals(causeException, resultingException);
    }

    @Test
    public void testRuntimeExceptionIsNotLostDuringMultiQuery(){
        RuntimeException causeException = new RuntimeException();
        MultiQueriableStepBatchErrorFetcher fetcher = new MultiQueriableStepBatchErrorFetcher(causeException);
        RuntimeException resultingException = Assertions.assertThrows(RuntimeException.class, () -> fetcher.fetchData(traversal, mockVertex, 0));
        Assertions.assertEquals(causeException, resultingException);
    }

    private static class MultiQueriableStepBatchErrorFetcher extends MultiQueriableStepBatchFetcher<Object>{

        private final RuntimeException exceptionToThrow;

        private MultiQueriableStepBatchErrorFetcher(RuntimeException exceptionToThrow) {
            super(0);
            this.exceptionToThrow = exceptionToThrow;
        }

        @Override
        protected Map<JanusGraphVertex, Object> makeQueryAndExecute(JanusGraphMultiVertexQuery multiQuery) {
            throw exceptionToThrow;
        }
    }

}
