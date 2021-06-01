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

package org.janusgraph.graphdb.vertices;

import io.github.artsok.RepeatedIfExceptionsTest;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StandardVertexTest {

    private final long defaultTimeoutMilliseconds = 5000;

    @Mock
    private StandardJanusGraphTx tx;

    @Mock
    private InternalRelation internalRelation;

    private StandardVertex standardVertex;

    @BeforeEach
    public void setup(){
        standardVertex = spy(new StandardVertex(tx, 1, (byte) 1));
    }

    @RepeatedIfExceptionsTest(repeats = 3, minSuccess = 1)
    public void shouldNotStuckInDeadlockWhenTheVerticeAndItsRelationIsDeletedInParallel()
        throws InterruptedException, TimeoutException, ExecutionException {

        ExecutorService es = Executors.newSingleThreadExecutor();

        Future<?> future = es.submit(() -> {
            when(internalRelation.isLoaded()).thenReturn(true);

            ReentrantLock lock = new ReentrantLock();
            Condition vertexRemoveCondition = lock.newCondition();
            Condition relationRemoveCondition = lock.newCondition();

            AtomicBoolean removeVertexCalled = new AtomicBoolean(false);
            AtomicBoolean removeRelationExecuted = new AtomicBoolean(false);

            doAnswer(invocation -> {

                lock.lock();

                try {
                    removeVertexCalled.set(true);
                    vertexRemoveCondition.signalAll();

                    while (!removeRelationExecuted.get()){
                        relationRemoveCondition.await();
                    }

                    standardVertex.updateLifeCycle(ElementLifeCycle.Event.REMOVED);

                } finally {
                    lock.unlock();
                }

                return null;
            }).when(standardVertex).remove();

            new Thread(() -> standardVertex.remove()).start();

            lock.lock();

            try{
                while (!removeVertexCalled.get()){
                    vertexRemoveCondition.await();
                }

                standardVertex.removeRelation(internalRelation);

                removeRelationExecuted.set(true);
                relationRemoveCondition.signalAll();
            } finally {
                lock.unlock();
            }
            return null;
        });

        future.get(defaultTimeoutMilliseconds, TimeUnit.MILLISECONDS);
    }

    @Test
    public void modifiedVertexShouldNotEvictedFromCache() {
        for (int i = 0; i < 50; i++) {
            try (StandardJanusGraph g =
                (StandardJanusGraph) JanusGraphFactory.build()
                                                      .set("storage.backend", "inmemory")
                                                      .set("cache.tx-cache-size", 0)
                                                      .open()) {
                Vertex v1 = g.traversal().addV().next();
                Vertex v2 = g.traversal().addV().next();
                v1.addEdge("E", v2);
                g.tx().commit();
                g.tx().close();
                for (int k = 0; k < 120; k++) {
                    g.traversal().addV().next();
                }
                g.tx().commit();
                g.tx().close();

                g.traversal().E().drop().iterate();
                g.traversal().E().drop().iterate();
            }
        }
    }
}
