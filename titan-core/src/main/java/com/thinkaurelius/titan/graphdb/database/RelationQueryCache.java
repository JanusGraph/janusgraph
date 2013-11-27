package com.thinkaurelius.titan.graphdb.database;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.RelationType;
import com.tinkerpop.blueprints.Direction;

import java.util.EnumMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class RelationQueryCache {

    private final Cache<Long,CacheEntry> cache;
    private final EdgeSerializer edgeSerializer;

    private final EnumMap<RelationType,SliceQuery> relationTypes;

    public RelationQueryCache(EdgeSerializer edgeSerializer) {
        this(edgeSerializer,256);
    }

    public RelationQueryCache(EdgeSerializer edgeSerializer, int capacity) {
        this.edgeSerializer = edgeSerializer;
        this.cache = CacheBuilder.newBuilder().maximumSize(capacity*3/2).initialCapacity(capacity)
                .concurrencyLevel(2).build();
        relationTypes = new EnumMap<RelationType, SliceQuery>(RelationType.class);
        for (RelationType rt : RelationType.values()) {
            relationTypes.put(rt,edgeSerializer.getQuery(rt));
        }
    }

    public SliceQuery getQuery(RelationType type) {
        return relationTypes.get(type);
    }

    public SliceQuery getQuery(final InternalType type, Direction dir) {
        CacheEntry ce;
        try {
            ce = cache.get(type.getID(),new Callable<CacheEntry>() {
                @Override
                public CacheEntry call() throws Exception {
                    return new CacheEntry(edgeSerializer,type);
                }
            });
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

        public CacheEntry(EdgeSerializer edgeSerializer, InternalType t) {
            if (t.isPropertyKey()) {
                out = edgeSerializer.getQuery(t, Direction.OUT,new EdgeSerializer.TypedInterval[0],null);
                in = out;
                both = out;
            } else {
                out = edgeSerializer.getQuery(t,Direction.OUT,
                            new EdgeSerializer.TypedInterval[t.getSortKey().length],null);
                in = edgeSerializer.getQuery(t,Direction.IN,
                        new EdgeSerializer.TypedInterval[t.getSortKey().length],null);
                both = edgeSerializer.getQuery(t,Direction.BOTH,
                        new EdgeSerializer.TypedInterval[t.getSortKey().length],null);
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
