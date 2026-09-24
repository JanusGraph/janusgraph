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

package org.janusgraph.graphdb.vertices;

import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.util.datastructures.Retriever;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

//A vertex caches the relations its queries read, and a commit which changes a schema element refreshes that cache on
//the element in every open transaction, while another thread which shares one of those transactions may be reading
//the element. A read which loaded the relations before the change and stored them after the refresh would undo it,
//and the transaction would go on acting on the old definition edges.
public class CacheVertexRefreshTest {

    //A loaded vertex; the transaction is never consulted
    private static CacheVertex loadedVertex() {
        return new CacheVertex(mock(StandardJanusGraphTx.class), 1L, ElementLifeCycle.Loaded) {
            @Override
            public boolean isNew() {
                return false;
            }
        };
    }

    @Test
    public void aLoadWhichARefreshOverlapsIsNotKept() throws Exception {
        final CacheVertex vertex = loadedVertex();
        final SliceQuery query = new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(1));
        final AtomicInteger loads = new AtomicInteger();
        final CountDownLatch loaded = new CountDownLatch(1);
        final CountDownLatch resume = new CountDownLatch(1);
        //Pauses after loading, so that a refresh can come in before the result is stored
        final Retriever<SliceQuery, EntryList> pausingLookup = q -> {
            loads.incrementAndGet();
            loaded.countDown();
            try {
                assertTrue(resume.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
            return EntryList.EMPTY_LIST;
        };
        final ExecutorService reader = Executors.newSingleThreadExecutor();
        try {
            final Future<EntryList> read = reader.submit(() -> vertex.loadRelations(query, pausingLookup));
            assertTrue(loaded.await(10, TimeUnit.SECONDS));
            vertex.refresh();
            resume.countDown();
            assertEquals(EntryList.EMPTY_LIST, read.get(10, TimeUnit.SECONDS));

            assertFalse(vertex.hasLoadedRelations(query), "a load the refresh overlapped was kept in the cache");
            vertex.loadRelations(query, pausingLookup);
            assertEquals(2, loads.get(), "the relations were not loaded again after the refresh");
            assertTrue(vertex.hasLoadedRelations(query), "a load after the refresh was not kept in the cache");
        } finally {
            reader.shutdownNow();
        }
    }

    //A multi-query loads the relations of many vertices in one read and hands each vertex its result afterwards,
    //with the refresh count it read before the read
    @Test
    public void aResultLoadedBeforeTheCallIsKeptOnlyIfNoRefreshCameSince() {
        final CacheVertex vertex = loadedVertex();
        final SliceQuery query = new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(1));
        final long refreshesBefore = vertex.refreshes();
        vertex.refresh();
        vertex.loadRelations(query, q -> EntryList.EMPTY_LIST, refreshesBefore);
        assertFalse(vertex.hasLoadedRelations(query), "a result loaded before a refresh was kept in the cache");

        vertex.loadRelations(query, q -> EntryList.EMPTY_LIST, vertex.refreshes());
        assertTrue(vertex.hasLoadedRelations(query), "a result loaded after the last refresh was not kept");
    }
}
