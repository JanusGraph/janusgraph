package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;

import java.nio.ByteBuffer;

/**
 * An entry is the primitive persistence unit used in the graph database storage backend.
 * <p/>
 * An entry consists of a column and value both of which are general {@link java.nio.ByteBuffer}s.
 * The value may be null but the column may not.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public interface Entry extends Comparable<Entry> {

    /**
     * Returns the column of this entry as a StaticBuffer.
     *
     * @return Column
     */
    public StaticBuffer getColumn();

    /**
     * Returns the value of this entry as a StaticBuffer.
     *
     * @return Value
     */
    public StaticBuffer getValue();

    /**
     * Returns the column of this entry as a ReadBuffer.
     *
     * @return Column
     */
    public ReadBuffer getReadColumn();

    /**
     * Returns the value of this entry as a ReadBuffer.
     *
     * @return Value
     */
    public ReadBuffer getReadValue();

    /**
     * Returns the column of this entry as an array.
     *
     * @return Column
     */
    public byte[] getArrayColumn();

    /**
     * Returns the value of this entry as an array.
     *
     * @return Value
     */
    public byte[] getArrayValue();

    /**
     * Returns the column of this entry as a ByteBuffer.
     *
     * @return Column
     */
    public ByteBuffer getByteBufferColumn();


    /**
     * Returns the value of this entry as a ByteBuffer.
     *
     * @return Value
     */
    public ByteBuffer getByteBufferValue();

    /**
     * Returns the cached parsed representation of this Entry if it exists, else NULL
     *
     * @return
     */
    public RelationCache getCache();

    /**
     * Sets the cached parsed representation of this Entry. This method does not synchronize,
     * so a previously set representation would simply be overwritten.
     *
     * @param cache
     */
    public void setCache(RelationCache cache);

}
