package com.thinkaurelius.titan.net.msg.handler;

import com.thinkaurelius.titan.net.Kernel;
import com.thinkaurelius.titan.net.SeedTracker;
import com.thinkaurelius.titan.net.msg.Fault;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dalaro
 */
public class FaultHandler implements Runnable {

    private final Kernel kernel;
    private final Fault fault;

    private static final Logger log =
            LoggerFactory.getLogger(FaultHandler.class);

    public FaultHandler(Kernel kernel, Fault fault) {
        this.kernel = kernel;
        this.fault = fault;
    }

    @Override
    public void run() {
        log.error("Fault received: " + fault);
        SeedTracker tracker = kernel.getQueryTracker(fault.getSeed());
        if (null == tracker) {
        	log.warn("Dropping unsolicited " + fault);
        }
        tracker.addFault(fault);
    }
}
