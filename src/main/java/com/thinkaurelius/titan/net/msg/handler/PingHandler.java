package com.thinkaurelius.titan.net.msg.handler;

import com.thinkaurelius.titan.net.InstanceWorklog;
import com.thinkaurelius.titan.net.Kernel;
import com.thinkaurelius.titan.net.msg.Key;
import com.thinkaurelius.titan.net.msg.Ping;
import com.thinkaurelius.titan.net.msg.Pong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author dalaro
 */
public class PingHandler implements Runnable {
    private final Kernel kernel;
    private final Ping ping;
    private static final Logger log =
            LoggerFactory.getLogger(PingHandler.class);

    public PingHandler(Kernel kernel, Ping ping) {
        this.kernel = kernel;
        this.ping = ping;
    }

    @Override
    public void run() {
    	InetSocketAddress pinger = ping.getClient().getHost();
    	
    	Set<Long> seeds = new HashSet<Long>();
    	for (InstanceWorklog worklog : kernel.getQueryWorklogs()) {
    		Key seed = worklog.getQuery().getSeed();
    		if (seed.getHost().equals(pinger))
    			seeds.add(seed.getId()); 
    	}
    	
    	Pong pong = new Pong(null, seeds);
    	kernel.send(pong, pinger);
    }
}
