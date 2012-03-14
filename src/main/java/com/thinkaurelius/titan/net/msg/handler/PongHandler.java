package com.thinkaurelius.titan.net.msg.handler;

import com.thinkaurelius.titan.net.Kernel;
import com.thinkaurelius.titan.net.SeedTracker;
import com.thinkaurelius.titan.net.msg.Key;
import com.thinkaurelius.titan.net.msg.Pong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 *
 * @author dalaro
 */
public class PongHandler implements Runnable {
    private final Kernel kernel;
    private final Pong pong;

    private static final Logger log =
            LoggerFactory.getLogger(PongHandler.class);

    public PongHandler(Kernel kernel, Pong pong) {
        this.kernel = kernel;
        this.pong = pong;
    }

	@Override
	public void run() {
		InetSocketAddress self = kernel.getListenAddress();
		long bootTime = kernel.getBootTime();
		for (Long l : pong.getQueryKeyIds()) {
			Key k = new Key(l, self, bootTime);
			SeedTracker tracker = kernel.getQueryTracker(k);
			if (null == tracker) {
				log.warn("Unknown query seed " + k + " cited in pong received from " + pong.getReplyAddress());
				continue;
			}
			//TODO tracker.setLastSeenTime(pong.getReplyTo(), now);
		}
	}
}
