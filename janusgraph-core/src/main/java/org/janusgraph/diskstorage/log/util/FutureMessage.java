package com.thinkaurelius.titan.diskstorage.log.util;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractFuture;
import com.thinkaurelius.titan.diskstorage.log.Message;

/**
 * Implementation of a {@link java.util.concurrent.Future} for {@link Message}s that
 * are being added to the {@link com.thinkaurelius.titan.diskstorage.log.Log} via {@link com.thinkaurelius.titan.diskstorage.log.Log#add(com.thinkaurelius.titan.diskstorage.StaticBuffer)}.
 *
 * This class can be used by {@link com.thinkaurelius.titan.diskstorage.log.Log} implementations to wrap messages.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FutureMessage<M extends Message> extends AbstractFuture<Message> {

    private final M message;

    public FutureMessage(M message) {
        Preconditions.checkNotNull(message);
        this.message = message;
    }

    /**
     * Returns the actual message that was added to the log
     * @return
     */
    public M getMessage() {
        return message;
    }

    /**
     * This method should be called by {@link com.thinkaurelius.titan.diskstorage.log.Log} implementations when the message was successfully
     * added to the log.
     */
    public void delivered() {
        super.set(message);
    }

    /**
     * This method should be called by {@Link Log} implementations when the message could not be added to the log
     * with the respective exception object.
     * @param exception
     */
    public void failed(Throwable exception) {
        super.setException(exception);
    }

    @Override
    public String toString() {
        return "FutureMessage[" + message + "]";
    }
}
