// Copyright 2018 JanusGraph Authors
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

import org.easymock.EasyMockSupport;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.util.datastructures.Retriever;
import org.junit.jupiter.api.Test;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class CacheVertexTest extends EasyMockSupport {

    @Test
    public void testLoadRelationsWithNullSuperSetValue() {
        final SliceQuery mockSliceQuery = createMock(SliceQuery.class);
        final Retriever mockRetriever = createMock(Retriever.class);

        expect(mockSliceQuery.subsumes(isA(SliceQuery.class))).andReturn(true);
        expect(mockRetriever.get(isA(SliceQuery.class))).andReturn(null);

        replayAll();

        final CacheVertex cacheVertex = createMockBuilder(CacheVertex.class)
            .withConstructor(createMock(StandardJanusGraphTx.class), 0L, (byte) 0)
            .addMockedMethod("isNew")
            .createMock();

        expect(cacheVertex.isNew()).andReturn(false);

        replay(cacheVertex);

        cacheVertex.addToQueryCache(mockSliceQuery, null);
        cacheVertex.loadRelations(createMock(SliceQuery.class), mockRetriever);

        verify(mockRetriever);
    }

}
