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

package org.janusgraph.graphdb.database.cache;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.types.system.BaseLabel;
import org.janusgraph.graphdb.types.system.BaseRelationType;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StandardSchemaCacheTest {

    private static final int CACHE_SIZE = 1;
    private static final int RELATION_CAPACITY = CACHE_SIZE * 3;

    private static final EntryList NON_EMPTY = StaticArrayEntryList.of(
        StaticArrayEntry.of(StaticArrayBuffer.of(new byte[]{1}), StaticArrayBuffer.of(new byte[]{2})));

    private static class CountingRetriever implements SchemaCache.StoreRetrieval {
        final AtomicInteger relationRetrievals = new AtomicInteger();
        final Set<Long> emptySchemaIds = new HashSet<>();

        @Override
        public Long retrieveSchemaByName(String typeName) {
            return null;
        }

        @Override
        public EntryList retrieveSchemaRelations(long schemaId, BaseRelationType type, Direction dir) {
            relationRetrievals.incrementAndGet();
            return emptySchemaIds.contains(schemaId) ? EntryList.EMPTY_LIST : NON_EMPTY;
        }
    }

    private static long edgeLabelId(long count) {
        return IDManager.getSchemaId(IDManager.VertexIDType.UserEdgeLabel, count);
    }

    private static void overflowPrimaryRelationMap(StandardSchemaCache cache) {
        for (long count = 1; count <= RELATION_CAPACITY + 2; count++) {
            cache.getSchemaRelations(edgeLabelId(count), BaseKey.SchemaName, Direction.OUT);
        }
    }

    @Test
    public void emptyDefinitionEdgesAreCachedInPrimaryMode() {
        CountingRetriever retriever = new CountingRetriever();
        StandardSchemaCache cache = new StandardSchemaCache(CACHE_SIZE, retriever);
        long unindexedLabel = edgeLabelId(1);
        retriever.emptySchemaIds.add(unindexedLabel);

        assertTrue(cache.getSchemaRelations(unindexedLabel, BaseLabel.SchemaDefinitionEdge, Direction.OUT).isEmpty());
        assertTrue(cache.getSchemaRelations(unindexedLabel, BaseLabel.SchemaDefinitionEdge, Direction.OUT).isEmpty());

        assertEquals(1, retriever.relationRetrievals.get());
    }

    @Test
    public void emptyDefinitionEdgesAreCachedAfterFallingBackToBoundedCache() {
        CountingRetriever retriever = new CountingRetriever();
        StandardSchemaCache cache = new StandardSchemaCache(CACHE_SIZE, retriever);
        overflowPrimaryRelationMap(cache);
        long unindexedLabel = edgeLabelId(RELATION_CAPACITY + 10);
        retriever.emptySchemaIds.add(unindexedLabel);

        int before = retriever.relationRetrievals.get();
        for (int i = 0; i < 5; i++) {
            assertTrue(cache.getSchemaRelations(unindexedLabel, BaseLabel.SchemaDefinitionEdge, Direction.OUT).isEmpty());
        }

        assertEquals(1, retriever.relationRetrievals.get() - before);
    }

    @Test
    public void emptyDefinitionPropertiesAreNotCachedInPrimaryMode() {
        CountingRetriever retriever = new CountingRetriever();
        StandardSchemaCache cache = new StandardSchemaCache(CACHE_SIZE, retriever);
        assertEmptyDefinitionPropertiesAreReReadUntilTheTypeAppears(cache, retriever, edgeLabelId(1));
    }

    @Test
    public void emptyDefinitionPropertiesAreNotCachedAfterFallingBackToBoundedCache() {
        CountingRetriever retriever = new CountingRetriever();
        StandardSchemaCache cache = new StandardSchemaCache(CACHE_SIZE, retriever);
        overflowPrimaryRelationMap(cache);
        assertEmptyDefinitionPropertiesAreReReadUntilTheTypeAppears(cache, retriever, edgeLabelId(RELATION_CAPACITY + 10));
    }

    /**
     * Every existing schema vertex has definition properties, so an empty result means the schema vertex is not
     * visible yet. It must not be remembered: a lookup that races the creation of the type would otherwise leave the
     * type with an empty definition until the cache is expired.
     */
    private static void assertEmptyDefinitionPropertiesAreReReadUntilTheTypeAppears(StandardSchemaCache cache,
                                                                                  CountingRetriever retriever,
                                                                                  long notYetVisibleLabel) {
        retriever.emptySchemaIds.add(notYetVisibleLabel);

        int before = retriever.relationRetrievals.get();
        for (int i = 0; i < 3; i++) {
            assertTrue(cache.getSchemaRelations(notYetVisibleLabel, BaseKey.SchemaDefinitionProperty, Direction.OUT).isEmpty());
        }
        assertEquals(3, retriever.relationRetrievals.get() - before);

        // the type becomes visible: the next lookup sees it and the answer is cached from then on
        retriever.emptySchemaIds.remove(notYetVisibleLabel);
        before = retriever.relationRetrievals.get();
        for (int i = 0; i < 3; i++) {
            assertEquals(NON_EMPTY, cache.getSchemaRelations(notYetVisibleLabel, BaseKey.SchemaDefinitionProperty, Direction.OUT));
        }
        assertEquals(1, retriever.relationRetrievals.get() - before);
    }

    @Test
    public void expireSchemaElementEvictsFallbackEntries() {
        CountingRetriever retriever = new CountingRetriever();
        StandardSchemaCache cache = new StandardSchemaCache(CACHE_SIZE, retriever);
        overflowPrimaryRelationMap(cache);
        long unindexedLabel = edgeLabelId(RELATION_CAPACITY + 10);
        retriever.emptySchemaIds.add(unindexedLabel);

        cache.getSchemaRelations(unindexedLabel, BaseLabel.SchemaDefinitionEdge, Direction.OUT);
        int before = retriever.relationRetrievals.get();
        cache.expireSchemaElement(unindexedLabel);
        cache.getSchemaRelations(unindexedLabel, BaseLabel.SchemaDefinitionEdge, Direction.OUT);

        assertEquals(1, retriever.relationRetrievals.get() - before);
    }

    /**
     * Serves what storage holds when asked, and can hold the next lookup at the point where it has read storage but
     * not returned yet, so that a test can commit a change and evict it in between.
     */
    private static class PausingRetriever implements SchemaCache.StoreRetrieval {
        volatile EntryList relations = EntryList.EMPTY_LIST;
        volatile Long schemaId;
        volatile boolean pauseNext;
        final CountDownLatch read = new CountDownLatch(1);
        final CountDownLatch resume = new CountDownLatch(1);

        @Override
        public Long retrieveSchemaByName(String typeName) {
            final Long result = schemaId;
            pauseIfArmed();
            return result;
        }

        @Override
        public EntryList retrieveSchemaRelations(long schemaId, BaseRelationType type, Direction dir) {
            final EntryList result = relations;
            pauseIfArmed();
            return result;
        }

        private void pauseIfArmed() {
            if (!pauseNext) return;
            pauseNext = false;
            read.countDown();
            try {
                assertTrue(resume.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
    }

    /**
     * Runs the lookup, lets it read storage, then commits a change and evicts it the way a schema commit does, and only
     * then lets the lookup return what it read. What the lookup read predates the change, so it must not stay cached.
     */
    private static <T> T raceAnEviction(PausingRetriever retriever, Callable<T> lookup, Runnable commitAndEvict)
        throws Exception {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            retriever.pauseNext = true;
            final Future<T> result = executor.submit(lookup);
            assertTrue(retriever.read.await(10, TimeUnit.SECONDS));
            commitAndEvict.run();
            retriever.resume.countDown();
            return result.get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    private static void assertAnEvictedRelationLookupIsNotCached(StandardSchemaCache cache, PausingRetriever retriever,
                                                                  long schemaId) throws Exception {
        final EntryList read = raceAnEviction(retriever,
            () -> cache.getSchemaRelations(schemaId, BaseLabel.SchemaDefinitionEdge, Direction.OUT),
            () -> {
                retriever.relations = NON_EMPTY;
                cache.expireSchemaElement(schemaId);
            });
        assertTrue(read.isEmpty());
        assertEquals(NON_EMPTY, cache.getSchemaRelations(schemaId, BaseLabel.SchemaDefinitionEdge, Direction.OUT));
    }

    @Test
    public void aRelationLookupWhichRacesAnEvictionIsNotCachedInPrimaryMode() throws Exception {
        final PausingRetriever retriever = new PausingRetriever();
        final StandardSchemaCache cache = new StandardSchemaCache(CACHE_SIZE, retriever);
        assertAnEvictedRelationLookupIsNotCached(cache, retriever, edgeLabelId(1));
    }

    @Test
    public void aRelationLookupWhichRacesAnEvictionIsNotCachedAfterFallingBackToBoundedCache() throws Exception {
        final PausingRetriever retriever = new PausingRetriever();
        final StandardSchemaCache cache = new StandardSchemaCache(CACHE_SIZE, retriever);
        retriever.relations = NON_EMPTY;
        overflowPrimaryRelationMap(cache);
        retriever.relations = EntryList.EMPTY_LIST;
        assertAnEvictedRelationLookupIsNotCached(cache, retriever, edgeLabelId(RELATION_CAPACITY + 10));
    }

    @Test
    public void aNameLookupWhichRacesAnEvictionIsNotCached() throws Exception {
        final PausingRetriever retriever = new PausingRetriever();
        final StandardSchemaCache cache = new StandardSchemaCache(CACHE_SIZE, retriever);
        final long renamed = edgeLabelId(1);
        retriever.schemaId = renamed;
        //The lookup finds the name, then the element is renamed and evicted before the lookup returns
        final Long read = raceAnEviction(retriever, () -> cache.getSchemaId("knows"), () -> {
            retriever.schemaId = null;
            cache.expireSchemaElement(renamed);
        });
        assertEquals(renamed, read);
        assertNull(cache.getSchemaId("knows"));
    }
}
