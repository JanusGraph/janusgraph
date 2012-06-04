package com.thinkaurelius.titan.diskstorage;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface IDAuthority {

    /**
     * Returns an array that specifies a block of ids, i.e. the ids between return[0] (inclusive) and return[1] (exclusive).
     * It is guaranteed that the block of ids for the particular partition id is uniquely assigned, that is,
     * the block of ids has not been previously and will not subsequently be assigned again when invoking this method
     * on the local or any remote machine that is connected to the underlying storage backend.
     *
     * In other words, this method has to ensure that ids are uniquely assigned per partition.
     *
     * @param partition Partition for which to request an id block. Must be bigger or equal to 0
     * @param blockSize The size of the partition block. The range of ids (i.e. return[1]-return[0]) is expected to be equal to blockSize - however, it may be smaller. Returned ids must be positive.
     * @return a range of ids for the particular partition
     */
    public long[] getIDBlock(int partition);

}
