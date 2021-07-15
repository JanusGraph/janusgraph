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

package org.janusgraph.graphdb.transaction;

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.ReadOnlyTransactionException;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StandardTransactionBuilderTest {

    private StandardJanusGraph graph;

    @BeforeEach
    void setUp() {
        graph = (StandardJanusGraph) JanusGraphFactory.open("inmemory");
    }

    @Test
    public void testPropertyPrefetching() {
        boolean graphWideEnabled = graph.getConfiguration().hasPropertyPrefetching();
        assertEquals(graphWideEnabled, ((StandardJanusGraphTx) graph.newTransaction()).getConfiguration().hasPropertyPrefetching());

        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.buildTransaction().propertyPrefetching(true).start();
        assertTrue(tx.getConfiguration().hasPropertyPrefetching());

        tx = (StandardJanusGraphTx) graph.buildTransaction().propertyPrefetching(false).start();
        assertFalse(tx.getConfiguration().hasPropertyPrefetching());
    }

    @Test
    public void testMultiQuery() {
        boolean graphWideEnabled = graph.getConfiguration().useMultiQuery();
        assertEquals(graphWideEnabled, ((StandardJanusGraphTx) graph.newTransaction()).getConfiguration().useMultiQuery());

        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.buildTransaction().multiQuery(true).start();
        assertTrue(tx.getConfiguration().useMultiQuery());

        tx = (StandardJanusGraphTx) graph.buildTransaction().multiQuery(false).start();
        assertFalse(tx.getConfiguration().useMultiQuery());
    }

    @Test
    public void testBatchLoading() {
        boolean graphWideEnabled = graph.getConfiguration().isBatchLoading();
        assertEquals(graphWideEnabled, ((StandardJanusGraphTx) graph.newTransaction()).getConfiguration().hasEnabledBatchLoading());

        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.buildTransaction().enableBatchLoading().start();
        assertTrue(tx.getConfiguration().hasEnabledBatchLoading());

        tx = (StandardJanusGraphTx) graph.buildTransaction().disableBatchLoading().start();
        assertFalse(tx.getConfiguration().hasEnabledBatchLoading());
    }

    @Test
    public void testReadOnly() {
        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.buildTransaction().readOnly().start();
        assertTrue(tx.getConfiguration().isReadOnly());

        ReadOnlyTransactionException ex = assertThrows(ReadOnlyTransactionException.class, () -> tx.addVertex());
        assertEquals("Cannot create new entities in read-only transaction", ex.getMessage());
    }

    @Test
    public void testConsistencyChecks() {
        StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.buildTransaction().consistencyChecks(true).start();
        assertTrue(tx.getConfiguration().hasVerifyUniqueness());

        tx = (StandardJanusGraphTx) graph.buildTransaction().consistencyChecks(false).start();
        assertFalse(tx.getConfiguration().hasVerifyUniqueness());
    }
}
