package com.thinkaurelius.titan.diskstorage.util;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Iterator over records in the storage backend. Behaves like a normal iterator
 * with an additional close method so that resources associated with this
 * iterator can be released.
 * 
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface RecordIterator<T> extends Iterator<T>, Closeable { }
