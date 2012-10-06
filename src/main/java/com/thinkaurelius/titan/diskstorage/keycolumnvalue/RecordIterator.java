package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.StorageException;

import java.util.Iterator;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface RecordIterator<T> {

    public boolean hasNext() throws StorageException;

    public T next() throws StorageException;

    public void close() throws StorageException;
    
}
