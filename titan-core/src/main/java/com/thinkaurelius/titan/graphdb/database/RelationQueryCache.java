package com.thinkaurelius.titan.graphdb.database;

import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.internal.RelationType;
import com.thinkaurelius.titan.graphdb.util.ConcurrentLRUCache;
import com.tinkerpop.blueprints.Direction;

import java.util.EnumMap;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class RelationQueryCache {

    private final ConcurrentLRUCache<CacheEntry> cache;
    private final EdgeSerializer edgeSerializer;

    private final EnumMap<RelationType,SliceQuery> relationTypes;

    public RelationQueryCache(EdgeSerializer edgeSerializer) {
        this(edgeSerializer,100);
    }

    public RelationQueryCache(EdgeSerializer edgeSerializer, int capacity) {
        this.edgeSerializer = edgeSerializer;
        this.cache = new ConcurrentLRUCache<CacheEntry>(capacity * 2, // upper is double capacity
                capacity + capacity / 3, // lower is capacity + 1/3
                capacity, // acceptable watermark is capacity
                100, true, false, // 100 items initial size + use only one thread for items cleanup
                null);
        relationTypes = new EnumMap<RelationType, SliceQuery>(RelationType.class);
        for (RelationType rt : RelationType.values()) {
            relationTypes.put(rt,edgeSerializer.getQuery(rt));
        }
    }

    public SliceQuery getQuery(RelationType type) {
        return relationTypes.get(type);
    }

    public SliceQuery getQuery(InternalType type, Direction dir) {
        CacheEntry ce = cache.get(type.getID());
        if (ce == null) {
            ce = new CacheEntry(edgeSerializer,type);
            CacheEntry old = cache.putIfAbsent(type.getID(),ce);
            if (old!=null) ce=old;
        }
        assert ce!=null;
        return ce.get(dir);
    }

    public void close() {
        cache.destroy();
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
