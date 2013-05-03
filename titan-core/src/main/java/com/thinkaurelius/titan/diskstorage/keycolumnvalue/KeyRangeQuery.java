package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.graphdb.query.Query;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.nio.ByteBuffer;

/**
 * Extends a {@link SliceQuery} by a key range which identifies the range of keys (start inclusive, end exclusive)
 * to which the slice query is applied.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class KeyRangeQuery extends SliceQuery {

    private final ByteBuffer keyStart;
    private final ByteBuffer keyEnd;


    public KeyRangeQuery(ByteBuffer keyStart, ByteBuffer keyEnd, ByteBuffer sliceStart, ByteBuffer sliceEnd, int limit, boolean isStatic) {
        super(sliceStart, sliceEnd, limit, isStatic);
        Preconditions.checkNotNull(keyStart);
        Preconditions.checkNotNull(keyEnd);
        this.keyStart=keyStart;
        this.keyEnd = keyEnd;
    }

    public KeyRangeQuery(ByteBuffer keyStart, ByteBuffer keyEnd, ByteBuffer sliceStart, ByteBuffer sliceEnd) {
        this(keyStart,keyEnd,sliceStart,sliceEnd, Query.NO_LIMIT,DEFAULT_STATIC);
    }

    public KeyRangeQuery(ByteBuffer keyStart, ByteBuffer keyEnd, ByteBuffer sliceStart, ByteBuffer sliceEnd, int limit) {
        this(keyStart,keyEnd,sliceStart,sliceEnd,limit,DEFAULT_STATIC);
    }

    public KeyRangeQuery(ByteBuffer keyStart, ByteBuffer keyEnd, ByteBuffer sliceStart, ByteBuffer sliceEnd, boolean isStatic) {
        this(keyStart, keyEnd, sliceStart, sliceEnd, Query.NO_LIMIT, isStatic);
    }

    public ByteBuffer getKeyStart() {
        return keyStart;
    }

    public ByteBuffer getKeyEnd() {
        return keyEnd;
    }

    @Override
    public int hashCode() {
        if (hashcode==0) {
            hashcode = new HashCodeBuilder().append(keyStart).append(keyEnd).appendSuper(super.hashCode()).toHashCode();
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
        return super.subsumes(oth) &&
                ByteBufferUtil.isSmallerOrEqualThan(keyStart, oth.keyStart) &&
                ByteBufferUtil.isSmallerOrEqualThan(oth.keyEnd,keyEnd);
    }
}
