package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.graphdb.query.Query;

/**
 * Extends {@link SliceQuery} by a key that identifies the location of the slice in the key-ring.
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class KeySliceQuery extends SliceQuery {

    private final StaticBuffer key;

    public KeySliceQuery(StaticBuffer key, StaticBuffer sliceStart, StaticBuffer sliceEnd, int limit, boolean isStatic) {
        super(sliceStart, sliceEnd, limit, isStatic);
        Preconditions.checkNotNull(key);
        this.key=key;
    }

    public KeySliceQuery(StaticBuffer key, SliceQuery query) {
        super(query);
        Preconditions.checkNotNull(key);
        this.key=key;
    }

    public KeySliceQuery(StaticBuffer key, StaticBuffer sliceStart, StaticBuffer sliceEnd) {
        this(key,sliceStart,sliceEnd, Query.NO_LIMIT,DEFAULT_STATIC);
    }

    public KeySliceQuery(StaticBuffer key, StaticBuffer sliceStart, StaticBuffer sliceEnd, int limit) {
        this(key,sliceStart,sliceEnd,limit,DEFAULT_STATIC);
    }

    public KeySliceQuery(StaticBuffer key, StaticBuffer sliceStart, StaticBuffer sliceEnd, boolean isStatic) {
        this(key,sliceStart,sliceEnd,Query.NO_LIMIT,isStatic);
    }

    /**
     *
     * @return the key of this query
     */
    public StaticBuffer getKey() {
        return key;
    }


    @Override
    public int hashCode() {
        if (hashcode==0) {
            hashcode = key.hashCode()*102329 + super.hashCode();
            if (hashcode==0) hashcode=1;
        }
        return hashcode;
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


}
