package com.thinkaurelius.titan.diskstorage.log;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
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
            reader.read(message);
        } catch (Throwable e) {
            log.error("Encountered exception when processing message [{}] by reader [{}]: {}",message, reader, e);
        }
    }
}
