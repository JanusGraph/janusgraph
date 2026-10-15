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

package org.janusgraph.graphdb.util;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_Traverser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class JanusGraphTraverserUtilTest {

    // TinkerPop's own traversers, one which counts loops and one which doesn't
    @Test
    public void testGetLoopsForTinkerPopTraversers() {
        Assertions.assertTrue(JanusGraphTraverserUtil.isLoopsPotentiallySupportedBy(B_LP_O_S_SE_SL_Traverser.class));
        Assertions.assertFalse(JanusGraphTraverserUtil.isLoopsPotentiallySupportedBy(B_O_Traverser.class));
        Assertions.assertEquals(2, JanusGraphTraverserUtil.getLoops(new CountingTraverser(2)));
        Assertions.assertEquals(0, JanusGraphTraverserUtil.getLoops(new B_O_Traverser<>("v", 1)));
    }

    // Threads which see traversers of a class which counts loops, of one whose loops() throws and of one which doesn't
    // count them, all at once; LoopCountingTraverser is seen nowhere else
    @Test
    public void testGetLoopsFromManyThreadsAtOnce() throws Exception {
        final ExecutorService threads = Executors.newFixedThreadPool(8);
        try {
            final CountDownLatch start = new CountDownLatch(1);
            final List<Future<Integer>> loops = new ArrayList<>();
            for (int i = 0; i < 64; i++) {
                final int n = i;
                loops.add(threads.submit(() -> {
                    if (!start.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("the start signal never came");
                    }
                    switch (n % 3) {
                        case 0: return JanusGraphTraverserUtil.getLoops(new LoopCountingTraverser(n));
                        case 1: return JanusGraphTraverserUtil.getLoops(new TestTraverserWithoutLoopsSupport<>());
                        default: return JanusGraphTraverserUtil.getLoops(new B_O_Traverser<>("v", 1));
                    }
                }));
            }
            start.countDown();
            for (int i = 0; i < loops.size(); i++) {
                Assertions.assertEquals(i % 3 == 0 ? i : 0, loops.get(i).get(10, TimeUnit.SECONDS), "task " + i);
            }
            Assertions.assertTrue(JanusGraphTraverserUtil.isLoopsPotentiallySupportedBy(LoopCountingTraverser.class));
        } finally {
            threads.shutdown();
            if (!threads.awaitTermination(10, TimeUnit.SECONDS)) {
                threads.shutdownNow();
                Assertions.fail("the tasks did not end");
            }
        }
    }

    private static class CountingTraverser extends B_LP_O_S_SE_SL_Traverser<String> {
        CountingTraverser(int loops) {
            super("v", EmptyStep.instance(), 1);
            initialiseLoops("repeat", null);
            for (int i = 0; i < loops; i++) {
                incrLoops();
            }
        }
    }

    private static final class LoopCountingTraverser extends CountingTraverser {
        LoopCountingTraverser(int loops) {
            super(loops);
        }
    }

    @Test
    public void testGetLoopsForTraverserWithoutLoopsSupportWhenPreviouslyAssumedToHaveLoopsSupport(){
        Assertions.assertEquals(0, JanusGraphTraverserUtil.getLoops(new TestTraverserWithoutLoopsSupport<>()));
    }

    @Test
    public void testGetLoopsForAnonymousTraverserWithLoopsSupport(){
        Assertions.assertEquals(5, JanusGraphTraverserUtil.getLoops(new Traverser<Object>() {
            @Override
            public Object get() {
                return null;
            }

            @Override
            public <S> S sack() {
                return null;
            }

            @Override
            public <S> void sack(S object) {
                //ignored
            }

            @Override
            public Path path() {
                return null;
            }

            @Override
            public int loops() {
                return 5;
            }

            @Override
            public int loops(String loopName) {
                return 0;
            }

            @Override
            public long bulk() {
                return 0;
            }

            @Override
            public Traverser<Object> clone() {
                return null;
            }
        }));
    }

    @Test
    public void testGetLoopsForAnonymousTraverserWithoutLoopsSupport(){
        Assertions.assertEquals(0, JanusGraphTraverserUtil.getLoops(new Traverser<Object>() {
            @Override
            public Object get() {
                return null;
            }

            @Override
            public <S> S sack() {
                return null;
            }

            @Override
            public <S> void sack(S object) {
                //ignored
            }

            @Override
            public Path path() {
                return null;
            }

            @Override
            public int loops() {
                throw new IllegalStateException("This test Traversal doesn't support loops");
            }

            @Override
            public int loops(String loopName) {
                return 0;
            }

            @Override
            public long bulk() {
                return 0;
            }

            @Override
            public Traverser<Object> clone() {
                return null;
            }
        }));
    }

}
