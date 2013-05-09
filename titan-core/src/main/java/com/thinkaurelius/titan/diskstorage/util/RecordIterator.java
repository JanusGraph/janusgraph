package com.thinkaurelius.titan.diskstorage.util;

import com.thinkaurelius.titan.diskstorage.StorageException;

/**
 * Iterator over records in the storage backend. Behaves like a normal iterator
 * with an additional close method so that resources associated with this iterator can be released. *
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface RecordIterator<T> {

    public boolean hasNext() throws StorageException;

    public T next() throws StorageException;

    public void close() throws StorageException;

}
