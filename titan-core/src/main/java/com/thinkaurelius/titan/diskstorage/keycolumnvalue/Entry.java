package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.util.datastructures.ImmutableLongObjectMap;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.nio.ByteBuffer;

/**
 * An entry is the primitive persistence unit used in the graph database middleware.
 * <p/>
 * An entry consists of a column and value both of which are general {@link java.nio.ByteBuffer}s.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public interface Entry extends Comparable<Entry> {

    /**
     * Returns the column ByteBuffer of this entry.
     *
     * @return Column ByteBuffer
     */
    public ByteBuffer getColumn();


    /**
     * Returns the value ByteBuffer of this entry.
     *
     * @return Value ByteBuffer
     */
    public ByteBuffer getValue();

}
