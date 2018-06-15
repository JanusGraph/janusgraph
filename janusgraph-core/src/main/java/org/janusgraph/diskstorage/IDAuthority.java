// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage;


import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.idassigner.IDBlockSizer;

import java.time.Duration;
import java.util.List;

/**
 * Handles the unique allocation of ids. Returns blocks of ids that are uniquely allocated to the caller so that
 * they can be used to uniquely identify elements. *
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface IDAuthority {

    /**
     * Returns a block of new ids in the form of {@link IDBlock}. It is guaranteed that
     * the block of ids for the particular partition id is uniquely assigned,
     * that is, the block of ids has not been previously and will not
     * subsequently be assigned again when invoking this method on the local or
     * any remote machine that is connected to the underlying storage backend.
     * <p>
     * In other words, this method has to ensure that ids are uniquely assigned
     * per partition.
     * <p>
     * It is furthermore guaranteed that any id of the returned IDBlock is smaller than the upper bound
     * for the given partition as read from the {@link IDBlockSizer} set on this IDAuthority and that the
     * number of ids returned is equal to the block size of the IDBlockSizer.
     *
     * @param partition
     *            Partition for which to request an id block
     * @param idNamespace namespace for ids within a partition
     * @param timeout
     *            When a call to this method is unable to return a id block
     *            before this timeout elapses, the implementation must give up
     *            and throw a {@code StorageException} ASAP
     * @return a range of ids for the {@code partition} parameter
     */
    IDBlock getIDBlock(int partition, int idNamespace, Duration timeout)
            throws BackendException;

    /**
     * Returns the lower and upper limits of the key range assigned to this local machine as an array with two entries.
     *
     * @return
     * @throws BackendException
     */
    List<KeyRange> getLocalIDPartition() throws BackendException;

    /**
     * Sets the {@link IDBlockSizer} to be used by this IDAuthority. The IDBlockSizer specifies the block size for
     * each partition guaranteeing that the same partition will always be assigned the same block size.
     * <p>
     * The IDBlockSizer cannot be changed for an IDAuthority that has already been used (i.e. after invoking {@link #getIDBlock(int, int, Duration)}.
     *
     * @param sizer The IDBlockSizer to be used by this IDAuthority
     */
    void setIDBlockSizer(IDBlockSizer sizer);

    /**
     * Closes the IDAuthority and any underlying storage backend.
     *
     * @throws BackendException
     */
    void close() throws BackendException;

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
    String getUniqueID();

    /**
     * Whether {@link #getIDBlock(int, int, Duration)} may be safely interrupted.
     *
     * @return true if interruption is allowed, false if it is not
     */
    boolean supportsInterruption();

}
