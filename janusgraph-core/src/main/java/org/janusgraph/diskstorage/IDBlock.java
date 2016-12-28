package com.thinkaurelius.titan.diskstorage;

/**
 * Represents a block of ids. {@link #numIds()} return how many ids are in this block and {@link #getId(long)} retrieves
 * the id at the given position, where the position must be smaller than the number of ids in this block (similar to array access).
 * </p>
 * Any IDBlock implementation must be completely thread-safe.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface IDBlock {

    /**
     * Number of ids in this block.
     *
     * @return
     */
    public long numIds();

    /**
     * Returns the id at the given index. Index must be non-negative and smaller than {@link #numIds()}.
     *
     * @param index
     * @return
     */
    public long getId(long index);

}
