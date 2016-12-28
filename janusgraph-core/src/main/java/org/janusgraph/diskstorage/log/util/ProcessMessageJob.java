package com.thinkaurelius.titan.diskstorage.log.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.log.Message;
import com.thinkaurelius.titan.diskstorage.log.MessageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for processing read messages with the registered message readers.
 * Simple implementation of a {@link Runnable}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ProcessMessageJob implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ProcessMessageJob.class);

    private final Message message;
    private final MessageReader reader;

    public ProcessMessageJob(final Message message, final MessageReader reader) {
        Preconditions.checkArgument(message!=null && reader!=null);
        this.message = message;
        this.reader = reader;
    }

    @Override
    public void run() {
        try {
            log.debug("Passing {} to {}", message, reader);
            reader.read(message);
        } catch (Throwable e) {
            log.error("Encountered exception when processing message ["+message+"] by reader ["+reader+"]:",e);
        }
    }
}
