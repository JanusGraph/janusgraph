// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.optimize;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphMultiQueryStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.MultiQueriable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JanusGraphMultiQueryStepTest {

    @ParameterizedTest
    @MethodSource("generateTestParameters")
    public void testClone(Traversal.Admin traversal, boolean limitBatchSize, Collection<MultiQueriable> clients) {
        JanusGraphMultiQueryStep originalStep = new JanusGraphMultiQueryStep(traversal, limitBatchSize);
        clients.forEach(originalStep::attachSameLoopClient);

        JanusGraphMultiQueryStep clone = originalStep.clone();

        assertEquals(limitBatchSize, clone.isLimitBatchSize());
        assertEquals(originalStep.getSameLoopClientSteps().size(), clone.getSameLoopClientSteps().size());
        assertTrue(clone.getSameLoopClientSteps().containsAll(originalStep.getSameLoopClientSteps()));
        assertTrue(originalStep.getSameLoopClientSteps().containsAll(clone.getSameLoopClientSteps()));

        assertEquals(originalStep.getNextLoopClientSteps().size(), clone.getNextLoopClientSteps().size());
        assertTrue(clone.getNextLoopClientSteps().containsAll(originalStep.getNextLoopClientSteps()));
        assertTrue(originalStep.getNextLoopClientSteps().containsAll(clone.getNextLoopClientSteps()));

        assertEquals(originalStep.getFirstLoopClientSteps().size(), clone.getFirstLoopClientSteps().size());
        assertTrue(clone.getFirstLoopClientSteps().containsAll(originalStep.getFirstLoopClientSteps()));
        assertTrue(originalStep.getFirstLoopClientSteps().containsAll(clone.getFirstLoopClientSteps()));
    }

    @ParameterizedTest
    @MethodSource("generateTestParameters")
    public void testReset(Traversal.Admin traversal, boolean limitBatchSize, Collection<MultiQueriable> clients) {
        JanusGraphMultiQueryStep originalStep = new JanusGraphMultiQueryStep(traversal, limitBatchSize);
        clients.forEach(originalStep::attachSameLoopClient);

        originalStep.reset();

        assertEquals(limitBatchSize, originalStep.isLimitBatchSize());
        assertEquals(originalStep.getSameLoopClientSteps().size(), clients.size());
        assertTrue(clients.containsAll(originalStep.getSameLoopClientSteps()));
        assertTrue(originalStep.getSameLoopClientSteps().containsAll(clients));
    }

    private static Stream<Arguments> generateTestParameters() {
        Traversal.Admin mockedTraversal = mock(Traversal.Admin.class);
        when(mockedTraversal.getTraverserSetSupplier()).thenReturn(TraverserSet::new);

        MultiQueriable mqA = mock(MultiQueriable.class);
        MultiQueriable mqB = mock(MultiQueriable.class);

        List<MultiQueriable> emptyClientList = Collections.emptyList();
        List<MultiQueriable> singleClientList = Collections.singletonList(mqA);
        List<MultiQueriable> multiClientList = Arrays.asList(mqA, mqB);

        return Arrays.stream(new Arguments[]{
            arguments(mockedTraversal, true, emptyClientList),
            arguments(mockedTraversal, false, emptyClientList),
            arguments(mockedTraversal, true, singleClientList),
            arguments(mockedTraversal, false, singleClientList),
            arguments(mockedTraversal, true, multiClientList),
            arguments(mockedTraversal, false, multiClientList)
        });
    }
}
