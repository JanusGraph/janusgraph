package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;

/**
 * Extends a {@link SliceQuery} by a key range which identifies the range of keys (start inclusive, end exclusive)
 * to which the slice query is applied.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class KeyRangeQuery extends SliceQuery {

    private final StaticBuffer keyStart;
    private final StaticBuffer keyEnd;


    public KeyRangeQuery(StaticBuffer keyStart, StaticBuffer keyEnd, StaticBuffer sliceStart, StaticBuffer sliceEnd, boolean isStatic) {
        super(sliceStart, sliceEnd, isStatic);
        Preconditions.checkNotNull(keyStart);
        Preconditions.checkNotNull(keyEnd);
        this.keyStart=keyStart;
        this.keyEnd = keyEnd;
    }

    public KeyRangeQuery(StaticBuffer keyStart, StaticBuffer keyEnd, SliceQuery query) {
        super(query);
        Preconditions.checkNotNull(keyStart);
        Preconditions.checkNotNull(keyEnd);
        this.keyStart=keyStart;
        this.keyEnd = keyEnd;
    }

    public KeyRangeQuery(StaticBuffer keyStart, StaticBuffer keyEnd, StaticBuffer sliceStart, StaticBuffer sliceEnd) {
        this(keyStart,keyEnd,sliceStart,sliceEnd,DEFAULT_STATIC);
    }


    public StaticBuffer getKeyStart() {
        return keyStart;
    }

    public StaticBuffer getKeyEnd() {
        return keyEnd;
    }

    @Override
    public KeyRangeQuery setLimit(int limit) {
        super.setLimit(limit);
        return this;
    }

    @Override
    public KeyRangeQuery updateLimit(int newLimit) {
        return new KeyRangeQuery(keyStart,keyEnd,this).setLimit(newLimit);
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
