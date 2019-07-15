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

package org.janusgraph.core;

import org.janusgraph.graphdb.tinkerpop.JanusGraphBlueprintsGraph;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class TransactionTest {
    private ExecutorService executorService = Executors.newFixedThreadPool(1);

    @Test
    void threadLocalTxMustBeCleaned() throws InterruptedException, ExecutionException {
        JanusGraph graph = JanusGraphFactory.open("inmemory");

        CompletableFuture.runAsync(() -> {
            graph.tx().commit();
            graph.tx().close();
        }, executorService).get();

        Assertions.assertDoesNotThrow(() -> {
            CompletableFuture.runAsync(
                () -> Assertions.assertNull(getThreadLocalTxs(graph).get()), executorService).get();
        });
        graph.close();
        executorService.shutdown();
    }

    private ThreadLocal getThreadLocalTxs(JanusGraph graph) {
        try {
            Field txs = JanusGraphBlueprintsGraph.class.getDeclaredField("txs");
            txs.setAccessible(true);
            return (ThreadLocal) txs.get(graph);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }
}