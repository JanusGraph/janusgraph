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

package org.janusgraph.graphdb;

import com.sleepycat.je.dbi.CursorImpl;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;

class BerkeleyTransactionTest {
    static {
        // Needs for ignore assert when cursor is closed during scan
        ClassLoader.getSystemClassLoader().setClassAssertionStatus(CursorImpl.class.getName(), false);
    }

    @Test
    void longRunningTxShouldBeRolledBack(@TempDir File dir) throws InterruptedException {
        JanusGraph graph = JanusGraphFactory.open("berkeleyje:" + dir.getAbsolutePath());

        GraphTraversalSource traversal = graph.traversal();
        for (int i = 0; i < 10; i++) {
            traversal.addV().property("a", "2").next();
        }
        traversal.tx().commit();

        GraphTraversalSource g = graph.tx().createThreadedTx().traversal();
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            g.V()
             .has("a", "2")
             .sideEffect(ignored -> {
                 try {
                     // artificially slow down the traversal execution so that
                     // this test has a chance to interrupt it before it finishes
                     Thread.sleep(100);
                 } catch (InterruptedException e) {
                     Thread.currentThread().interrupt();
                 }
             })
             .toList();
        });
        Thread.sleep(100);
        g.tx().rollback();
        graph.close();
        assertThrows(ExecutionException.class, future::get);
    }
}
