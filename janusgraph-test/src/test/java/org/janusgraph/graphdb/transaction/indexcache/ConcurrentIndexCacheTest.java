// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.graphdb.transaction.indexcache;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.relations.StandardVertexProperty;
import org.janusgraph.graphdb.transaction.addedrelations.ConcurrentAddedRelations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A key's index which one thread builds on its first lookup while another thread adds or removes a property of the
 * key the way a transaction does: in its added relations first, then in the cache. The change reaches the added
 * relations at either moment which matters, just before the build reads them or just after, and either way the index
 * ends up holding what the added relations hold.
 */
public class ConcurrentIndexCacheTest {

    private static final long TIMEOUT_SECONDS = 10;

    private final PropertyKey uid = mock(PropertyKey.class);
    private final JanusGraphVertex newVertex = mock(JanusGraphVertex.class);
    private final ExecutorService otherThread = Executors.newSingleThreadExecutor();

    public enum Moment { BEFORE_THE_BUILD_READS, AFTER_THE_BUILD_READS }

    public ConcurrentIndexCacheTest() {
        when(newVertex.isNew()).thenReturn(true);
    }

    @AfterEach
    public void shutDown() {
        otherThread.shutdownNow();
    }

    //The added relations of a transaction which several threads use, which run a task in the thread which builds an
    //index, at the moment given, when the build reads them
    private static final class InterleavingAddedRelations extends ConcurrentAddedRelations {
        private final Moment moment;
        private Runnable task;

        private InterleavingAddedRelations(Moment moment) {
            super(false);
            this.moment = moment;
        }

        @Override
        public Iterable<InternalRelation> getViewOfProperties(Predicate<InternalRelation> filter) {
            runTaskIf(Moment.BEFORE_THE_BUILD_READS);
            final Iterable<InternalRelation> view = super.getViewOfProperties(filter);
            runTaskIf(Moment.AFTER_THE_BUILD_READS);
            return view;
        }

        private void runTaskIf(Moment now) {
            if (now == moment && task != null) {
                final Runnable once = task;
                task = null;
                once.run();
            }
        }
    }

    private StandardVertexProperty property(Object value) {
        final StandardVertexProperty property = mock(StandardVertexProperty.class);
        when(property.isProperty()).thenReturn(true);
        when(property.getType()).thenReturn(uid);
        when(property.propertyKey()).thenReturn(uid);
        when(property.value()).thenReturn(value);
        when(property.element()).thenReturn(newVertex);
        return property;
    }

    //Has the other thread change the added relations, waits until it has, and lets it go on to change the cache, which
    //it can do only once the build is over
    private Runnable inTheOtherThread(Runnable changeOfTheAddedRelations, Runnable changeOfTheCache,
                                      Future<?>[] change) {
        return () -> {
            final CountDownLatch changed = new CountDownLatch(1);
            change[0] = otherThread.submit(() -> {
                changeOfTheAddedRelations.run();
                changed.countDown();
                changeOfTheCache.run();
            });
            try {
                assertTrue(changed.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        };
    }

    @ParameterizedTest
    @EnumSource(Moment.class)
    public void aPropertyAddedWhileTheFirstLookupBuildsTheIndexIsIndexedOnce(Moment moment) throws Exception {
        final InterleavingAddedRelations added = new InterleavingAddedRelations(moment);
        final ConcurrentIndexCache cache = new ConcurrentIndexCache(added);
        final StandardVertexProperty before = property("a");
        added.add(before);
        cache.add(before);
        final StandardVertexProperty meanwhile = property("b");
        final Future<?>[] change = new Future<?>[1];
        added.task = inTheOtherThread(() -> added.add(meanwhile), () -> cache.add(meanwhile), change);

        //The first lookup builds the index, and the other thread adds its property meanwhile
        assertEquals(1, cache.count(uid, "a", true));
        change[0].get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertEquals(1, cache.count(uid, "b", true));
        assertEquals(ImmutableSet.of(meanwhile), ImmutableSet.copyOf(cache.get(uid, "b", true)));
        assertEquals(ImmutableSet.of(before, meanwhile), ImmutableSet.copyOf(cache.getAll(uid, true)));
        assertEquals(ImmutableSet.copyOf(added.getViewOfProperties(r -> true)), ImmutableSet.copyOf(cache.getAll(uid, true)));
    }

    @ParameterizedTest
    @EnumSource(Moment.class)
    public void aPropertyRemovedWhileTheFirstLookupBuildsTheIndexIsNotIndexed(Moment moment) throws Exception {
        final InterleavingAddedRelations added = new InterleavingAddedRelations(moment);
        final ConcurrentIndexCache cache = new ConcurrentIndexCache(added);
        final StandardVertexProperty stays = property("a");
        final StandardVertexProperty goes = property("b");
        for (StandardVertexProperty property : new StandardVertexProperty[] {stays, goes}) {
            added.add(property);
            cache.add(property);
        }
        final Future<?>[] change = new Future<?>[1];
        added.task = inTheOtherThread(() -> added.remove(goes), () -> cache.remove(goes), change);

        //The first lookup builds the index, and the other thread removes its property meanwhile
        assertEquals(1, cache.count(uid, "a", true));
        change[0].get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertEquals(0, cache.count(uid, "b", true));
        assertTrue(cache.get(uid, "b", true).isEmpty());
        assertEquals(ImmutableSet.of(stays), ImmutableSet.copyOf(cache.getAll(uid, true)));
        assertEquals(ImmutableSet.copyOf(added.getViewOfProperties(r -> true)), ImmutableSet.copyOf(cache.getAll(uid, true)));
    }
}
