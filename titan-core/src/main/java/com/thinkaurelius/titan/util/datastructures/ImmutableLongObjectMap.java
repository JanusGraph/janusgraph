package com.thinkaurelius.titan.util.datastructures;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.LongObjectCursor;

import java.util.*;

/**
 * Immutable map from long key ids to objects.
 * Implemented for memory and time efficiency.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ImmutableLongObjectMap implements Iterable<LongObjectCursor<Object>> {

    private final LongObjectOpenHashMap<Object> container;

    protected ImmutableLongObjectMap(final LongObjectOpenHashMap<Object> container) {
        this.container = container;
    }

    @SuppressWarnings("unchecked")
    public <O> O get(long key) {
        return (O) container.get(key);
    }

    public int size() {
        return container.size();
    }

    @Override
    public Iterator<LongObjectCursor<Object>> iterator() {
        return container.iterator();
    }

    public static class Builder {
        private static final int INITIAL_CAPACITY = 4;

        private final LongObjectOpenHashMap<Object> container;

        public Builder() {
            container = new LongObjectOpenHashMap<Object>(INITIAL_CAPACITY);
        }

        public void put(long key, Object value) {
            if (!container.putIfAbsent(key, value))
                throw new IllegalArgumentException("duplicate key found: " + key);
        }

        public int size() {
            return container.size();
        }

        public ImmutableLongObjectMap build() {
            return new ImmutableLongObjectMap(container);
        }
    }
}
