package com.thinkaurelius.titan.diskstorage.log;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;

import java.util.concurrent.Future;

/**
 * Represents a log that allows content to be added to it in the form of messages and to
 * read messages and their content from the log via registered {@link MessageReader}s.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Log {


    /**
     * Attempts to add the given content to the log and returns a {@link Future} for this action.
     * @param content
     * @return
     */
    public Future<Message> add(StaticBuffer content);

    /**
     * Attempts to add the given content to the log and returns a {@link Future} for this action.
     * In addition, a key is provided to signal the recipient of the log message in partitioned logging systems.
     *
     * @param content
     * @return
     */
    public Future<Message> add(StaticBuffer content, StaticBuffer key);

    /**
     *
     * @param reader
     * @see #registerReaders(Iterable)
     */
    public void registerReader(MessageReader... reader);

    /**
     * Registers the given readers with this log. These readers will be invoked for each newly read message from the log.
     * If no previous readers were registered, invoking this method triggers reader threads to be instantiated.
     *
     * @param readers
     */
    public void registerReaders(Iterable<MessageReader> readers);

    /**
     * Removes the given reader from the list of registered readers and returns whether this reader was registered in the
     * first place.
     * Note, that removing the last reader does not stop the reading process. Use {@link #close()} instead.
     *
     * @param reader
     * @return true if this MessageReader was registered before and was successfully unregistered, else false
     */
    public boolean unregisterReader(MessageReader reader);

    /**
     * Returns the name of this log
     * @return
     */
    public String getName();

    /**
     * Closes this log and stops the reading process.
     *
     * @throws StorageException
     */
    public void close() throws StorageException;

}
