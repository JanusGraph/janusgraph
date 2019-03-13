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

import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.junit.jupiter.api.BeforeEach;
import org.janusgraph.testutil.FlakyTest;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.mockito.Mockito.*;

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

    @FlakyTest(minSuccess = 1, invocationCount = 3)
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
}
