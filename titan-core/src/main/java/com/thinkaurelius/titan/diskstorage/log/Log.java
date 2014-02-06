package com.thinkaurelius.titan.diskstorage.log;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;

import java.util.concurrent.Future;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Log {

    public Future<Message> add(StaticBuffer payLoad);

    public Future<Message> add(StaticBuffer payLoad, StaticBuffer key);

    public void registerReader(MessageReader... reader);

    public void registerReaders(Iterable<MessageReader> readers);

    /**
     *
     * @param reader
     * @return true if this MessageReader was registered before and was successfully unregistered, else false
     */
    public boolean unregisterReader(MessageReader reader);

    public String getName();

    /**
     * Closes this log
     *
     * @throws StorageException
     */
    public void close() throws StorageException;

}
