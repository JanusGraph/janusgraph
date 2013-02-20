package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.nio.ByteBuffer;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class KeySliceQuery extends SliceQuery {

    private final ByteBuffer key;

    public KeySliceQuery(ByteBuffer key, ByteBuffer sliceStart, ByteBuffer sliceEnd, int limit, boolean isStatic) {
        super(sliceStart, sliceEnd, limit, isStatic);
        Preconditions.checkNotNull(key);
        this.key=key;
    }

    public KeySliceQuery(ByteBuffer key, SliceQuery query) {
        super(query);
        Preconditions.checkNotNull(key);
        this.key=key;
    }

    public KeySliceQuery(ByteBuffer key, ByteBuffer sliceStart, ByteBuffer sliceEnd) {
        this(key,sliceStart,sliceEnd,DEFAULT_LIMIT,DEFAULT_STATIC);
    }

    public KeySliceQuery(ByteBuffer key, ByteBuffer sliceStart, ByteBuffer sliceEnd, int limit) {
        this(key,sliceStart,sliceEnd,limit,DEFAULT_STATIC);
    }

    public KeySliceQuery(ByteBuffer key, ByteBuffer sliceStart, ByteBuffer sliceEnd, boolean isStatic) {
        this(key,sliceStart,sliceEnd,DEFAULT_LIMIT,isStatic);
    }

    /**
     *
     * @return the key of this query
     */
    public ByteBuffer getKey() {
        return key;
    }


    @Override
    public int hashCode() {
        if (hashcode==0) {
            hashcode = new HashCodeBuilder().append(key).appendSuper(super.hashCode()).toHashCode();
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
