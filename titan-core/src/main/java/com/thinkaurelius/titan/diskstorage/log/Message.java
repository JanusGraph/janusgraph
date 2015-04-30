package com.thinkaurelius.titan.diskstorage.log;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Messages which are added to and received from the {@link Log}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Message {

    /**
     * Returns the unique identifier for the sender of the message
     * @return
     */
    public String getSenderId();

    /**
     * Returns the timestamp of this message in the specified time unit.
     * This is the time when the message was added to the log.
     * @param unit
     * @return
     */
    public Instant getTimestamp();

    /**
     * Returns the content of the message
     * @return
     */
    public StaticBuffer getContent();

}
