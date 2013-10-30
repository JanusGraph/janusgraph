package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;

import java.nio.ByteBuffer;

/**
 * Implementation of {@link Entry} based on {@link StaticBuffer} column and value.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class StaticBufferEntry implements Entry {

    public static final StaticBuffer NO_VALUE = null;

    private final StaticBuffer column;
    private final StaticBuffer value;

    public StaticBufferEntry(StaticBuffer column, StaticBuffer value) {
        Preconditions.checkNotNull(column);
        this.column = column;
        this.value = value;
    }

    public static Entry of(StaticBuffer column) {
        return new StaticBufferEntry(column, NO_VALUE);
    }

    public static Entry of(StaticBuffer column, StaticBuffer value) {
        return new StaticBufferEntry(column, value);
    }

    @Override
    public StaticBuffer getColumn() {
        return column;
    }

    @Override
    public StaticBuffer getValue() {
        return value;
    }

    @Override
    public ReadBuffer getReadColumn() {
        return column.asReadBuffer();
    }

    @Override
    public ReadBuffer getReadValue() {
        return value.asReadBuffer();
    }

    @Override
    public byte[] getArrayColumn() {
        return column.as(StaticBuffer.ARRAY_FACTORY);
    }

    @Override
    public byte[] getArrayValue() {
        return value.as(StaticBuffer.ARRAY_FACTORY);
    }

    @Override
    public ByteBuffer getByteBufferColumn() {
        return column.asByteBuffer();
    }

    @Override
    public ByteBuffer getByteBufferValue() {
        return value.asByteBuffer();
    }


    @Override
    public int hashCode() {
        return getColumn().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (!(obj instanceof Entry)) return false;
        Entry other = (Entry) obj;
        return getColumn().equals(other.getColumn());
    }

    @Override
    public String toString() {
        return column.toString() + "->" + value.toString();
    }

    @Override
    public int compareTo(Entry entry) {
        return column.compareTo(entry.getColumn());
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
