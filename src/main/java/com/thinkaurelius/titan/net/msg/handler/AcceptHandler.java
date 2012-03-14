package com.thinkaurelius.titan.net.msg.handler;

import com.thinkaurelius.titan.net.IndividualQuerySender;
import com.thinkaurelius.titan.net.Kernel;
import com.thinkaurelius.titan.net.msg.Accept;
import com.thinkaurelius.titan.net.msg.Query;
import com.thinkaurelius.titan.net.msg.Trace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes a single Query-Acceptance message.
 *
 * @author dalaro
 */
public class AcceptHandler implements Runnable {

    private final Kernel kernel;
    private final Accept accept;
        private static final Logger log =
            LoggerFactory.getLogger(AcceptHandler.class);

    public AcceptHandler(Kernel kernel, Accept accept) {
    	this.kernel = kernel;
        this.accept = accept;
    }

    @Override
    public void run() {
        IndividualQuerySender qth = kernel.cancelForwardingTimeout(accept.getInstance());
        if (null == qth) {
            // Too late, forwarding the query to this node timed out already
            // For now, just log this as an error
            log.warn("Ignoring Accept unsolicited or received after timeout: " + accept);
        } else {
        	Query q = qth.getQuery();
            log.debug("Remote peer " + accept.getReplyAddress() + " accepted " + q.getInstance());
            // TODO replace this convoluted condition (if-i-am-not-the-seeder)
            //      with a get() on some map holding datastructures held by
            //      the client to track each seeded query
            if (q.isTracingEnabled() && !kernel.getListenAddress().equals(q.getSeed().getHost())) {
                // send end trace to client
                Trace t = new Trace(q.getSeed(), q.getInstance(), Trace.Code.END);
                kernel.send(t, q.getClient());
            }
        }
    }
}
