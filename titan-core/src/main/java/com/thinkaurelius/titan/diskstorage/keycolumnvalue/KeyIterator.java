package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface KeyIterator extends RecordIterator<StaticBuffer> {

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
