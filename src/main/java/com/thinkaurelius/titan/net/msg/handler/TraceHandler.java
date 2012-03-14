package com.thinkaurelius.titan.net.msg.handler;

import com.thinkaurelius.titan.net.Kernel;
import com.thinkaurelius.titan.net.msg.Accept;
import com.thinkaurelius.titan.net.msg.Key;
import com.thinkaurelius.titan.net.msg.Query;
import com.thinkaurelius.titan.net.msg.Trace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dalaro
 */
public class TraceHandler implements Runnable {

    private final Kernel kernel;
    private final Trace t;

    private static final Logger log =
            LoggerFactory.getLogger(TraceHandler.class);

    public TraceHandler(Kernel kernel, Trace trace) {
        this.kernel = kernel;
        this.t = trace;
    }

    @Override
    public void run() {
        log.debug("From " + t.getReplyAddress() + ": " + t);

        switch (t.getCode()) {
            case BEGIN: handleBegin(t); break;
            case END: handleEnd(t); break;
            case OK: handleOk(t); break;
        }
    }

    private void handleBegin(Trace t) {
        // Mark sender as "working" on the traced query
        kernel.getQueryTracker(t.getSeed()).
                addWorker(t.getInstance(), t.getInstance().getHost());

        // Reply with trace acknowledgement
        Trace reply = new Trace(t.getSeed(), t.getInstance(), Trace.Code.OK);
        kernel.send(reply, t.getReplyAddress());
    }

    private void handleOk(Trace t) {
        Key qInstance = t.getInstance();
        Query q = kernel.unholdQuery(qInstance);
        if (null == q) {
            log.warn("Received superfluous " + t +
                    " without associated local query from " + t.getReplyAddress());
            return;
        }

        assert qInstance.equals(q.getInstance());

        // Enqueue query for processing
        kernel.runqueueQuery(qInstance);

        // Tell forwarding worker we've accepted the query
        Accept a = new Accept(qInstance);
        kernel.send(a, qInstance.getHost());
    }

    private void handleEnd(Trace t) {
        kernel.getQueryTracker(t.getSeed()).removeWorker(t.getInstance());
    }
}
