package com.thinkaurelius.titan.diskstorage.log;

/**
 * Implementations of this interface are used to process messages read from the log.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface MessageReader {

    /**
     * Processes the given message. The message object may not be mutated!
     * @param message
     */
    public void read(Message message);

    /**
     * Need to override this method because the {@link Log} uses this comparison
     * when unregistering readers
     *
     * @param other other reader to compare against
     * @return
     */
    @Override
    public boolean equals(Object other);

}
