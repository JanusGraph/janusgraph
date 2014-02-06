package com.thinkaurelius.titan.diskstorage.log;

import com.thinkaurelius.titan.diskstorage.StorageException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface LogManager {

    /**
     * Opens a log for the given name and starts reading from it at the given readMarker once a {@link MessageReader}
     * is registered.
     * <p/>
     * If a log with the given name already exists, the existing log is returned and the readMarker is ignored.
     *
     * @param name
     * @param readMarker
     * @return
     * @throws StorageException
     */
    public Log openLog(String name, ReadMarker readMarker) throws StorageException;

    /**
     * Closes the log manager and all open logs
     *
     * @throws StorageException
     */
    public void close() throws StorageException;

}
