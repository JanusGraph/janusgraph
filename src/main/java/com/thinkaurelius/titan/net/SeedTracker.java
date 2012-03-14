package com.thinkaurelius.titan.net;

import com.thinkaurelius.titan.core.query.ResultCollector;
import com.thinkaurelius.titan.net.msg.Fault;
import com.thinkaurelius.titan.net.msg.Key;
import com.thinkaurelius.titan.net.msg.Query;
import com.thinkaurelius.titan.net.msg.Trace;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author dalaro
 */
public class SeedTracker {

	public static enum Status {
		CREATED, // No workers known to be answering query yet
		EXECUTING, // At least one worker is trying to answer
		DONE; // Query processing has terminated, regardless of error/success
	}

	private final Kernel kernel;
    private final Query query;
    private final ConcurrentHashMap<Key, InetSocketAddress> workers;
    private final BlockingQueue<byte[]> results;
    private final BlockingQueue<Fault> faults;
    private final BlockingQueue<Trace> traces;
    private final AtomicReference<Status> status;
    private final CountDownLatch doneLatch;
    private final ResultCollector<?> resultCollector;
    private final Class<?> resultClass;

    SeedTracker(Kernel kernel, Query query, ResultCollector<?> resultCollector, Class<?> resultClass) {
    	this.kernel = kernel;
        this.query = query;
        this.resultCollector = resultCollector;
        this.resultClass = resultClass;
        workers = new ConcurrentHashMap<Key, InetSocketAddress>();
        results = new LinkedBlockingQueue<byte[]>();
        faults = new LinkedBlockingQueue<Fault>();
        traces = new LinkedBlockingQueue<Trace>();
        status = new AtomicReference<Status>(Status.CREATED);
        doneLatch = new CountDownLatch(1);
    }

    public void addWorker(Key instance, InetSocketAddress host) {
        workers.put(instance, host);
        status.set(Status.EXECUTING);
    }

    public void removeWorker(Key instance) {
        workers.remove(instance);
        if (0 == workers.size()) {
            status.set(Status.DONE);
            doneLatch.countDown();
        }
    }
    
    public void addResult(byte[] result) {
    	results.add(result);
    	if (null == result)
    		return;
        Object o =
        	kernel.getSerializer().readObjectNotNull(ByteBuffer.wrap(result), resultClass);
        resultCollector.added(o);
    }
    
    public void addTrace(Trace t) {
    	traces.add(t);
    }
    
    public void addFault(Fault f) {
    	faults.add(f);
    }

    public Key getSeed() {
    	return getQuery().getSeed();
    }
    
    public Query getQuery() {
    	return query;
    }

    public boolean isDone() {
        return 0 == doneLatch.getCount();
    }
    
    public void waitUntilDone() throws InterruptedException {
    	doneLatch.await();
    }
    
    public BlockingQueue<byte[]> getResults() {
        return results;
    }
    
    public BlockingQueue<Trace> getTraces() {
    	return traces;
    }
    
    public BlockingQueue<Fault> getFaults() {
    	return faults;
    }
}
