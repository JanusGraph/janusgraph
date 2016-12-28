package com.thinkaurelius.titan.diskstorage.log;

import com.thinkaurelius.titan.diskstorage.BackendException;

/**
 * Manager interface for opening {@link Log}s against a particular Log implementation.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface LogManager {

    /**
     * Opens a log for the given name.
     * <p/>
     * If a log with the given name already exists, the existing log is returned.
     *
     * @param name Name of the log to be opened
     * @return
     * @throws com.thinkaurelius.titan.diskstorage.BackendException
     */
    public Log openLog(String name) throws BackendException;

    /**
     * Closes the log manager and all open logs (if they haven't already been explicitly closed)
     *
     * @throws com.thinkaurelius.titan.diskstorage.BackendException
     */
    public void close() throws BackendException;

}
