package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.graphdb.query.Query;

/**
 * Extends a {@link SliceQuery} by a key range which identifies the range of keys (start inclusive, end exclusive)
 * to which the slice query is applied.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class KeyRangeQuery extends SliceQuery {

    private final StaticBuffer keyStart;
    private final StaticBuffer keyEnd;


    public KeyRangeQuery(StaticBuffer keyStart, StaticBuffer keyEnd, StaticBuffer sliceStart, StaticBuffer sliceEnd, int limit, boolean isStatic) {
        super(sliceStart, sliceEnd, limit, isStatic);
        Preconditions.checkNotNull(keyStart);
        Preconditions.checkNotNull(keyEnd);
        this.keyStart=keyStart;
        this.keyEnd = keyEnd;
    }

    public KeyRangeQuery(StaticBuffer keyStart, StaticBuffer keyEnd, StaticBuffer sliceStart, StaticBuffer sliceEnd) {
        this(keyStart,keyEnd,sliceStart,sliceEnd, Query.NO_LIMIT,DEFAULT_STATIC);
    }

    public KeyRangeQuery(StaticBuffer keyStart, StaticBuffer keyEnd, StaticBuffer sliceStart, StaticBuffer sliceEnd, int limit) {
        this(keyStart,keyEnd,sliceStart,sliceEnd,limit,DEFAULT_STATIC);
    }

    public KeyRangeQuery(StaticBuffer keyStart, StaticBuffer keyEnd, StaticBuffer sliceStart, StaticBuffer sliceEnd, boolean isStatic) {
        this(keyStart, keyEnd, sliceStart, sliceEnd, Query.NO_LIMIT, isStatic);
    }

    public StaticBuffer getKeyStart() {
        return keyStart;
    }

    public StaticBuffer getKeyEnd() {
        return keyEnd;
    }

    @Override
    public int hashCode() {
        if (hashcode==0) {
            hashcode = keyStart.hashCode()*102329 + keyEnd.hashCode()*234331 + super.hashCode();
            if (hashcode==0) hashcode=1;
        }
        return hashcode;
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        KeyRangeQuery oth = (KeyRangeQuery)other;
        return keyStart.equals(oth.keyStart) && keyEnd.equals(oth.keyEnd) && super.equals(oth);
    }

    public boolean subsumes(KeyRangeQuery oth) {
        return super.subsumes(oth) && keyStart.compareTo(oth.keyStart)<=0 && keyEnd.compareTo(oth.keyEnd)>=0;
    }
}
