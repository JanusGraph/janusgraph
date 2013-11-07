package com.thinkaurelius.titan.graphdb.database.idassigner;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface IDBlockSizer {

    /**
     * The size of the id block to be returned by calls {@link com.thinkaurelius.titan.diskstorage.IDAuthority#getIDBlock(int)}.
     * In other words, for the returned array of the above mentioned call, it must hold that the difference between the second
     * and first value is equal to the block size returned by this method (for the same partition id).
     *
     * @param partitionID
     * @return
     */
    public long getBlockSize(int partitionID);

    /**
     * Returns the upper bound for any id block returned by {@link com.thinkaurelius.titan.diskstorage.IDAuthority#getIDBlock(int)}.
     * In other words, it must hold that the second value of the returned array is smaller than this value for the same partition id.
     *
     * @param partitionID
     * @return
     */
    public long getIdUpperBound(int partitionID);

}
