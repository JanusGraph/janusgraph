// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.graphdb.database;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.RelationCategory;

import java.util.EnumMap;
import java.util.concurrent.ExecutionException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class RelationQueryCache implements AutoCloseable {

    private final Cache<Long,CacheEntry> cache;
    private final EdgeSerializer edgeSerializer;

    private final EnumMap<RelationCategory,SliceQuery> relationTypes;

    public RelationQueryCache(EdgeSerializer edgeSerializer) {
        this(edgeSerializer,256);
    }

    public RelationQueryCache(EdgeSerializer edgeSerializer, int capacity) {
        this.edgeSerializer = edgeSerializer;
        this.cache = CacheBuilder.newBuilder().maximumSize(capacity*3/2).initialCapacity(capacity)
                .concurrencyLevel(2).build();
        relationTypes = new EnumMap<>(RelationCategory.class);
        for (RelationCategory rt : RelationCategory.values()) {
            relationTypes.put(rt,edgeSerializer.getQuery(rt,false));
        }
    }

    public SliceQuery getQuery(RelationCategory type) {
        return relationTypes.get(type);
    }

    public SliceQuery getQuery(final InternalRelationType type, Direction dir) {
        CacheEntry ce;
        try {
            ce = cache.get(type.longId(), () -> new CacheEntry(edgeSerializer,type));
        } catch (ExecutionException e) {
            throw new AssertionError("Should not happen: " + e.getMessage());
        }
        assert ce!=null;
        return ce.get(dir);
    }

    public void close() {
        cache.invalidateAll();
        cache.cleanUp();
    }

    private static final class CacheEntry {

        private final SliceQuery in;
        private final SliceQuery out;
        private final SliceQuery both;

        public CacheEntry(EdgeSerializer edgeSerializer, InternalRelationType t) {
            if (t.isPropertyKey()) {
                out = edgeSerializer.getQuery(t, Direction.OUT,new EdgeSerializer.TypedInterval[t.getSortKey().length]);
                in = out;
                both = out;
            } else {
                out = edgeSerializer.getQuery(t,Direction.OUT,
                            new EdgeSerializer.TypedInterval[t.getSortKey().length]);
                in = edgeSerializer.getQuery(t,Direction.IN,
                        new EdgeSerializer.TypedInterval[t.getSortKey().length]);
                both = edgeSerializer.getQuery(t,Direction.BOTH,
                        new EdgeSerializer.TypedInterval[t.getSortKey().length]);
            }
        }

        public SliceQuery get(Direction dir) {
            switch (dir) {
                case IN: return in;
                case OUT: return out;
                case BOTH: return both;
                default: throw new AssertionError("Unknown direction: " + dir);
            }
        }

    }

}
