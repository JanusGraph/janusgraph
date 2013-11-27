package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Extends {@link SliceQuery} by a key that identifies the location of the slice in the key-ring.
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class KeySliceQuery extends SliceQuery {

    private final StaticBuffer key;

    public KeySliceQuery(StaticBuffer key, StaticBuffer sliceStart, StaticBuffer sliceEnd, boolean isStatic) {
        super(sliceStart, sliceEnd, isStatic);
        Preconditions.checkNotNull(key);
        this.key=key;
    }

    public KeySliceQuery(StaticBuffer key, SliceQuery query) {
        super(query);
        Preconditions.checkNotNull(key);
        this.key=key;
    }

    public KeySliceQuery(StaticBuffer key, StaticBuffer sliceStart, StaticBuffer sliceEnd) {
        this(key,sliceStart,sliceEnd,DEFAULT_STATIC);
    }

    /**
     *
     * @return the key of this query
     */
    public StaticBuffer getKey() {
        return key;
    }

    @Override
    public KeySliceQuery setLimit(int limit) {
        super.setLimit(limit);
        return this;
    }

    @Override
    public KeySliceQuery updateLimit(int newLimit) {
        return new KeySliceQuery(key,this).setLimit(newLimit);
    }


    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(key).appendSuper(super.hashCode()).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        KeySliceQuery oth = (KeySliceQuery)other;
        return key.equals(oth.key) && super.equals(oth);
    }

    public boolean subsumes(KeySliceQuery oth) {
        return key.equals(oth.key) && super.subsumes(oth);
    }

    @Override
    public String toString() {
        return String.format("KeySliceQuery(key: %s, start: %s, end: %s, limit:%d)", key, getSliceStart(), getSliceEnd(), getLimit());
    }
}
