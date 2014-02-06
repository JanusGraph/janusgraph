package com.thinkaurelius.titan.diskstorage.log;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractFuture;
import com.thinkaurelius.titan.diskstorage.log.Message;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FutureMessage<M extends Message> extends AbstractFuture<Message> {

    private final M message;

    public FutureMessage(M message) {
        Preconditions.checkNotNull(message);
        this.message = message;
    }

    public M getMessage() {
        return message;
    }

    public void delivered() {
        super.set(message);
    }

    public void failed(Throwable exception) {
        super.setException(exception);
    }

}
