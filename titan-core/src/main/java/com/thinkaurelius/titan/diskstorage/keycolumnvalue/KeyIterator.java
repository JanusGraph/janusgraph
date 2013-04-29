package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import java.nio.ByteBuffer;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface KeyIterator extends RecordIterator<ByteBuffer> {

    /**
     * Returns an iterator over all entries associated with the current
     * key that match the column range specified in the query.
     * </p>
     * Note, that closing the returned sub-iterator closes this iterator.
     *
     * @return
     */
    public RecordIterator<Entry> getEntries();

}
