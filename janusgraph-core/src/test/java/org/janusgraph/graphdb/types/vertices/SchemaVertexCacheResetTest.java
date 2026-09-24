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

package org.janusgraph.graphdb.types.vertices;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.SchemaSource;
import org.janusgraph.graphdb.types.TypeDefinitionCategory;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

//A commit which changes a schema element resets its caches in every open transaction, while another thread which shares
//one of those transactions may be reading the element. A read which loaded the element's related elements before the
//change and stores them after the reset would undo the reset, and the transaction would go on acting on the old
//definition edges, for instance creating a second copy of a connection constraint.
public class SchemaVertexCacheResetTest {

    private static final TypeDefinitionCategory CONNECTION = TypeDefinitionCategory.UPDATE_CONNECTION_EDGE;

    //Loads whatever the test has put in place, and pauses after loading while a latch is set
    private static class PausingSchemaVertex extends JanusGraphSchemaVertex {
        volatile ListMultimap<TypeDefinitionCategory, Entry> related = ImmutableListMultimap.of();
        volatile CountDownLatch loaded;
        volatile CountDownLatch resume;

        PausingSchemaVertex() {
            super(mock(StandardJanusGraphTx.class), 1L, ElementLifeCycle.Loaded);
        }

        @Override
        ListMultimap<TypeDefinitionCategory, Entry> loadRelated(Direction dir) {
            final ListMultimap<TypeDefinitionCategory, Entry> result = related;
            final CountDownLatch pause = resume;
            if (pause != null) {
                loaded.countDown();
                try {
                    assertTrue(pause.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }
            return result;
        }
    }

    @Test
    public void shouldNotKeepRelatedElementsLoadedBeforeAnOverlappingReset() throws Exception {
        final PausingSchemaVertex vertex = new PausingSchemaVertex();
        final CountDownLatch resume = new CountDownLatch(1);
        vertex.loaded = new CountDownLatch(1);
        vertex.resume = resume;

        //Loads the connections as they were before the change, then waits before storing them, on a thread of its own
        final ExecutorService reader = Executors.newSingleThreadExecutor();
        try {
            final Future<Integer> overlapped = reader.submit(() -> vertex.getRelated(CONNECTION, Direction.OUT).size());
            assertTrue(vertex.loaded.await(10, TimeUnit.SECONDS));

            //The change is committed, and resets the element while that read is paused
            vertex.related = ImmutableListMultimap.of(CONNECTION, new SchemaSource.Entry(vertex, null));
            vertex.resume = null;
            vertex.resetCache();
            resume.countDown();

            //The read which began before the reset gets what it loaded; any read after the reset gets the change
            assertEquals(0, overlapped.get(10, TimeUnit.SECONDS));
            assertEquals(1, vertex.getRelated(CONNECTION, Direction.OUT).size());
        } finally {
            reader.shutdownNow();
        }
    }
}
