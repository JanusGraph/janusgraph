package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;

import java.nio.ByteBuffer;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface IDAuthority {

    /**
     * Returns an array that specifies a block of idAuthorities, i.e. the idAuthorities between return[0] (inclusive) and return[1] (exclusive).
     * It is guaranteed that the block of idAuthorities for the particular partition id is uniquely assigned, that is,
     * the block of idAuthorities has not been previously and will not subsequently be assigned again when invoking this method
     * on the local or any remote machine that is connected to the underlying storage backend.
     * <p/>
     * In other words, this method has to ensure that idAuthorities are uniquely assigned per partition.
     *
     * @param partition Partition for which to request an id block. Must be bigger or equal to 0
     * @return a range of idAuthorities for the particular partition
     */
    public long[] getIDBlock(int partition) throws StorageException;

    /**
     * Returns the smallest not yet allocated id for the given partition
     *
     * @param partition
     * @return
     * @throws StorageException
     */
    public long peekNextID(int partition) throws StorageException;

    /**
     * Returns the lower and upper limits of the key range assigned to this local machine as an array with two entries.
     *
     * @return
     * @throws StorageException
     */
    public ByteBuffer[] getLocalIDPartition() throws StorageException;

    /**
     * Sets the {@link IDBlockSizer} to be used by this IDAuthority. The IDBlockSizer specifies the block size for
     * each partition guaranteeing that the same partition will always be assigned the same block size.
     * <p/>
     * The IDBlockSizer cannot be changed for an IDAuthority that has already been used (i.e. after invoking {@link #getIDBlock(int)}.
     *
     * @param sizer The IDBlockSizer to be used by this IDAuthority
     */
    public void setIDBlockSizer(IDBlockSizer sizer);

    /**
     * Closes the IDAuthority and any underlying storage backend.
     *
     * @throws StorageException
     */
    public void close() throws StorageException;


}
