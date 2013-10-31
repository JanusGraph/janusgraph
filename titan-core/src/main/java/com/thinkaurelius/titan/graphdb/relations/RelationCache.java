package com.thinkaurelius.titan.graphdb.relations;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.tinkerpop.blueprints.Direction;

import java.util.*;

/**
 * Immutable map from long key ids to objects.
 * Implemented for memory and time efficiency.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class RelationCache implements Iterable<LongObjectCursor<Object>> {

    private static final LongObjectOpenHashMap<Object> EMPTY = new LongObjectOpenHashMap<Object>(0);

    public final Direction direction;
    public final long typeId;
    public final long relationId;
    private final Object other;
    private final LongObjectOpenHashMap<Object> properties;

    public RelationCache(final Direction direction, final long typeId, final long relationId,
                         final Object other, final LongObjectOpenHashMap<Object> properties) {
        this.direction = direction;
        this.typeId = typeId;
        this.relationId = relationId;
        this.other = other;
        this.properties = (properties == null || properties.size() > 0) ? properties : EMPTY;
    }

    @SuppressWarnings("unchecked")
    public <O> O get(long key) {
        return (O) properties.get(key);
    }

    public boolean hasProperties() {
        return properties != null;
    }

    public int numProperties() {
        return properties.size();
    }

    public Object getValue() {
        return other;
    }

    public Long getOtherVertexId() {
        return (Long) other;
    }

    public Iterator<LongObjectCursor<Object>> propertyIterator() {
        return properties.iterator();
    }

    @Override
    public Iterator<LongObjectCursor<Object>> iterator() {
        return propertyIterator();
    }

}
