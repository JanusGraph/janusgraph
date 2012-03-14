package com.thinkaurelius.titan.net.msg.handler;

import com.thinkaurelius.titan.net.Kernel;
import com.thinkaurelius.titan.net.SeedTracker;
import com.thinkaurelius.titan.net.msg.Key;
import com.thinkaurelius.titan.net.msg.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 *
 * @author dalaro
 */
public class ResultHandler implements Runnable {

    private final Kernel kernel;
    private final Result r;

    private static final Logger log =
            LoggerFactory.getLogger(ResultHandler.class);

    public ResultHandler(Kernel kernel, Result result) {
        this.kernel = kernel;
        this.r = result;
    }

    @Override
    public void run() {
        InetSocketAddress worker = r.getReplyAddress();
        Key k = r.getSeed();

        log.debug("Stored " + r + " from worker " + worker);

        SeedTracker tracker = kernel.getQueryTracker(k);
        byte[] result = r.getDataByteArray();
        tracker.addResult(result);
    }

}
