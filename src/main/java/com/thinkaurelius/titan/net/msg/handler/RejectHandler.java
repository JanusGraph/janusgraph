package com.thinkaurelius.titan.net.msg.handler;

import com.thinkaurelius.titan.net.IndividualQuerySender;
import com.thinkaurelius.titan.net.Kernel;
import com.thinkaurelius.titan.net.msg.Reject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dalaro
 */
public class RejectHandler implements Runnable {

    private final Kernel kernel;
    private final Reject reject;

    private static final Logger log =
            LoggerFactory.getLogger(RejectHandler.class);

    public RejectHandler(Kernel kernel, Reject reject) {
        this.kernel = kernel;
        this.reject = reject;
    }

    @Override
    public void run() {
        IndividualQuerySender qth = kernel.cancelForwardingTimeout(reject.getInstance());
        if (null == qth) {
            // Too late, forwarding the query to this node timed out already
            // For now, just log this as an error
        	log.warn("Ignoring Reject unsolicited or received after timeout: " + reject);
        } else {
        	// Immediately try next destination candidate for the Query
            kernel.sendQuery(qth);
        }
    }
}
