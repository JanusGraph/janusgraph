package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Handles the unique allocation of ids. Returns blocks of ids that are uniquely allocated to the caller so that
 * they can be used to uniquely identify elements. *
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface IDAuthority {

    /**
     * Returns an array that specifies a block of new ids, i.e. the ids between
     * return[0] (inclusive) and return[1] (exclusive). It is guaranteed that
     * the block of ids for the particular partition id is uniquely assigned,
     * that is, the block of ids has not been previously and will not
     * subsequently be assigned again when invoking this method on the local or
     * any remote machine that is connected to the underlying storage backend.
     * <p/>
     * In other words, this method has to ensure that ids are uniquely assigned
     * per partition.
     *
     * @param partition
     *            Partition for which to request an id block
     * @param timeout
     *            When a call to this method is unable to return a id block
     *            before this timeout elapses, the implementation must give up
     *            and throw a {@code StorageException} ASAP
     * @param TimeUnit
     *            Units associated with the {@code timeout} parameter
     * @return a range of ids for the {@code partition} parameter
     */
    public long[] getIDBlock(int partition, long timeout, TimeUnit unit)
            throws StorageException;

    /**
     * Returns the lower and upper limits of the key range assigned to this local machine as an array with two entries.
     *
     * @return
     * @throws StorageException
     */
    public List<KeyRange> getLocalIDPartition() throws StorageException;

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

    /**
     * Return the globally unique string used by this {@code IDAuthority}
     * instance to recognize its ID allocations and distinguish its allocations
     * from those belonging to other {@code IDAuthority} instances.
     *
     * This should normally be the value of
     * {@link GraphDatabaseConfiguration#UNIQUE_INSTANCE_ID}, though that's not
     * strictly technically necessary.
     *
     * @return unique ID string
     */
    public String getUniqueID();


}
