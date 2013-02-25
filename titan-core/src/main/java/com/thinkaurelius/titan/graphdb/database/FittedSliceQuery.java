package com.thinkaurelius.titan.graphdb.database;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;

import java.nio.ByteBuffer;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class FittedSliceQuery extends SliceQuery {

    private final boolean isFitted;

    public FittedSliceQuery(boolean isFitted, ByteBuffer sliceStart, ByteBuffer sliceEnd, int limit, boolean isStatic) {
        super(sliceStart, sliceEnd, limit, isStatic);
        this.isFitted=isFitted;
    }
//
//    public FittedSliceQuery(boolean isFitted, ByteBuffer sliceStart, ByteBuffer sliceEnd) {
//        super(sliceStart, sliceEnd);
//        this.isFitted=isFitted;
//    }
//
//    public FittedSliceQuery(boolean isFitted, ByteBuffer sliceStart, ByteBuffer sliceEnd, int limit) {
//        super(sliceStart, sliceEnd, limit);
//        this.isFitted=isFitted;
//    }
//
//    public FittedSliceQuery(boolean isFitted, ByteBuffer sliceStart, ByteBuffer sliceEnd, boolean isStatic) {
//        super(sliceStart, sliceEnd, isStatic);
//        this.isFitted=isFitted;
//    }

    public boolean isFitted() {
        return isFitted;
    }

}
