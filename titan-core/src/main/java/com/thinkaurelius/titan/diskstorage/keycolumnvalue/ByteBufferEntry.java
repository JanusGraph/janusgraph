package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.ReadByteBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;

import java.nio.ByteBuffer;

/**
 * Implementation of {@link Entry} based on {@link ByteBuffer} column and value.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class ByteBufferEntry implements Entry {

    public static final ByteBuffer NO_VALUE = null;

    private final ByteBuffer column;
    private final ByteBuffer value;

    public ByteBufferEntry(ByteBuffer column, ByteBuffer value) {
        Preconditions.checkNotNull(column);
        this.column = column;
        this.value = value;
    }

    public static Entry of(ByteBuffer column, ByteBuffer value) {
        return new ByteBufferEntry(column, value);
    }

    @Override
    public StaticBuffer getColumn() {
        return new StaticByteBuffer(column);
    }

    @Override
    public StaticBuffer getValue() {
        return new StaticByteBuffer(value);
    }

    @Override
    public ReadBuffer getReadColumn() {
        return new ReadByteBuffer(column);
    }

    @Override
    public ReadBuffer getReadValue() {
        return new ReadByteBuffer(value);
    }

    @Override
    public byte[] getArrayColumn() {
        return ByteBufferUtil.getArray(column);
    }

    @Override
    public byte[] getArrayValue() {
        return ByteBufferUtil.getArray(value);
    }

    @Override
    public ByteBuffer getByteBufferColumn() {
        return column.duplicate();
    }

    @Override
    public ByteBuffer getByteBufferValue() {
        return value.duplicate();
    }

    @Override
    public int hashCode() {
        return ByteBufferUtil.hashcode(column);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (!(obj instanceof Entry)) return false;
        if (obj instanceof ByteBufferEntry) {
            return ByteBufferUtil.equals(column, ((ByteBufferEntry) obj).column);
        } else {
            return getColumn().equals(((Entry) obj).getColumn());
        }
    }

    @Override
    public String toString() {
        return ByteBufferUtil.toString(column, "-") + "->" + ByteBufferUtil.toString(value, "-");
    }

    @Override
    public int compareTo(Entry entry) {
        if (entry instanceof ByteBufferEntry) {
            return ByteBufferUtil.compare(column, ((ByteBufferEntry) entry).column);
        } else {
            return ByteBufferUtil.compare(getColumn(), entry.getColumn());
        }
    }

    /**
     * ############# IDENTICAL CODE ###############
     */

    private volatile transient RelationCache cache;

    @Override
    public RelationCache getCache() {
        return cache;
    }

    @Override
    public void setCache(RelationCache cache) {
        Preconditions.checkNotNull(cache);
        this.cache = cache;
    }

}
