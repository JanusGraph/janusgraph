package com.thinkaurelius.titan.net.msg.handler;

import com.thinkaurelius.titan.net.Kernel;
import com.thinkaurelius.titan.net.msg.Accept;
import com.thinkaurelius.titan.net.msg.Query;
import com.thinkaurelius.titan.net.msg.Reject;
import com.thinkaurelius.titan.net.msg.Trace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process the next query in the incoming query queue.
 *
 * @author dalaro
 */
public class QueryHandler implements Runnable {

    private final Kernel kernel;
    private final Query q;

    private static final Logger logger =
            LoggerFactory.getLogger(QueryHandler.class);

    public QueryHandler(Kernel kernel, Query q) {
        this.kernel = kernel;
        this.q = q;
    }

    @Override
	public void run() {
    	
		logger.debug("From " + q.getSender() + ": " + q );

		// Check the list of failed query ids
		if (kernel.isSeedKilled(q.getSeed())) {
			reject(q, Reject.Code.BLACKLIST);
			return;
		}

		// Check queue size
		if (!kernel.isQueryCapacityFree()) {
			reject(q, Reject.Code.BUSY);
			return;
		}

		kernel.registerQuery(q);

		// // Case 1/2: received a brand new query sent by the client
		// if (q.getClient().equals(q.getReplyAddress())) {
		// accept(q);
		// trace(q);
		// } else { // Case 2/2: received forwarded query passed by a cluster
		// node
		// if (q.isTracingEnabled()) {
		// trace(q);
		// } else {
		// accept(q);
		// }
		// }

		if (q.isTracingEnabled()) {
			trace(q);
		} else {
			accept(q);
		}
	}

    private void reject(Query q, Reject.Code c) {
        Reject r = new Reject(q.getInstance(), c);
        kernel.send(r, q.getReplyAddress());
    }

    private void accept(Query q) {
        kernel.runqueueQuery(q.getInstance());

        Accept a = new Accept(q.getInstance());
        kernel.send(a, q.getReplyAddress());
    }

    private void trace(Query q) {
        Trace t = new Trace(q.getSeed(), q.getInstance(), Trace.Code.BEGIN);
        kernel.holdQuery(q.getInstance());
        kernel.send(t, q.getClient());
    }
}
