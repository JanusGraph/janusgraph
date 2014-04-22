package com.thinkaurelius.titan.diskstorage.log;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * Manager interface for opening {@link Log}s against a particular Log implementation.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface LogManager {

    /**
     * Opens a log for the given name and starts reading from it at the given readMarker once a {@link MessageReader}
     * is registered.
     * <p/>
     * If a log with the given name already exists, the existing log is returned and the readMarker is ignored.
     *
     * @param name Name of the log to be opened
     * @param readMarker Indicates where to start reading from the log once message readers are registered
     * @return
     * @throws StorageException
     */
    public Log openLog(String name, ReadMarker readMarker) throws StorageException;

    /**
     * Closes the log manager and all open logs (if they haven't already been explicitly closed)
     *
     * @throws StorageException
     */
    public void close() throws StorageException;

}
