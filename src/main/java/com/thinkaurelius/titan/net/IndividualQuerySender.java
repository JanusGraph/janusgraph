package com.thinkaurelius.titan.net;

import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.net.msg.Key;
import com.thinkaurelius.titan.net.msg.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Created with a list of possible destinations for a query
 * that must be sent/forwarded over the network, this Runnable
 * attempts to send the query to the next possible destination
 * upon each invocation of run().  It also schedules itself
 * on the Kernel's main executor to run again after TIMEOUT_MILLIS
 * seconds have passed.  When an Accept message is received
 * from a destination, this Runnable should be canceled immediately.
 *
 * @author dalaro
 */
public class IndividualQuerySender implements Runnable {

    private final Kernel kernel;
    private final List<InetSocketAddress> destinationsTried;
    private final List<InetSocketAddress> destinationCandidates;
    private final Key instanceKey;
    private final Query query;
    private ScheduledFuture<?> myNextScheduledRun;
    
    public static final long TIMEOUT_MILLIS = 10000; // ms
    
    private static final Logger log = 
    	LoggerFactory.getLogger(IndividualQuerySender.class);

    public IndividualQuerySender(Kernel kernel, Query query, List<InetSocketAddress> destinations) {
    	this.kernel = kernel;
    	this.destinationCandidates = new LinkedList<InetSocketAddress>(destinations);
    	this.destinationsTried = new LinkedList<InetSocketAddress>();
    	this.query = query;
    	this.instanceKey = this.query.getInstance();
	}
    
    public Key getInstance() {
    	return instanceKey;
    }
    
    public Query getQuery() {
    	return query;
    }
    
    @Override
    public void run() {
    	log.debug("Running; dests=" + destinationCandidates.size() +
    			" failed=" + destinationsTried.size());
    	
    	// If we've already tried a destination, then log a debug message
    	if (0 < destinationsTried.size()) {
    		InetSocketAddress failed =
    			Iterators.getLast(destinationsTried.iterator());
    		log.debug("Remote peer " + failed + 
    				  " timed out or rejected query instance " + 
    				  query.getInstance());
    	}
    	
    	// Try to send out the query if possible
    	if (0 < destinationCandidates.size()) {
            tryNextQueryCandidate();
    	} else {
    		int candidatesTried = destinationsTried.size();
    		log.debug("Query forwarding failed. " +
    				  "Tried " + candidatesTried + " destinations for " +
    				  query + " but none responded.");
    		// TODO check query mode and send notice to client/kill query
    		
    		// old code
            // TODO shoot query in the head if the mode bit is set
//            Fault f = new Fault(base.getSeed(), "Failed to reach any of the query forwarding candidates; query cannot be completed: " + oq.getCandidates());
//            kernel.send(f, base.getClient());
    	}
    	
    }
    
    public boolean cancelFuture() {
    	if (null == myNextScheduledRun)
    		return true; // We never tried to run, so true seems least surprising
    	
    	return myNextScheduledRun.cancel(false);
    }

    public void tryNextQueryCandidate() {
    	InetSocketAddress dest = destinationCandidates.remove(0);

		log.debug("Attempting to send " + query + " to " + dest);
        kernel.send(query, dest);
    	myNextScheduledRun = 
    		kernel.scheduleExecution(this, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        
        destinationsTried.add(dest);
    }

}
