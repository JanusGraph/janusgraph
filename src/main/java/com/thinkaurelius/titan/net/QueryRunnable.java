package com.thinkaurelius.titan.net;

import com.thinkaurelius.titan.core.GraphTransaction;
import com.thinkaurelius.titan.core.GraphTransactionConfig;
import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.core.query.QueryResult;
import com.thinkaurelius.titan.core.query.QueryType;
import com.thinkaurelius.titan.graphdb.database.GraphDB;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.net.msg.Query;
import com.thinkaurelius.titan.net.msg.Result;
import com.thinkaurelius.titan.net.msg.Trace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author dalaro
 */
public class QueryRunnable implements Runnable {

    private final Kernel kernel;
    private final Query q;
    private final GraphDB gdb;
    
    private static final int SERIALIZER_CAPACITY_GUESS = 128;

    private static final Logger log =
            LoggerFactory.getLogger(QueryRunnable.class);

    public QueryRunnable(Kernel kernel, Query q, GraphDB gdb) {
        this.kernel = kernel;
        this.q = q;
        this.gdb = gdb;
    }

    @Override
    public void run() {
        kernel.startQuery(q.getInstance());
        try {
            answerQuery();
        } catch (IOException e) {
            log.error("Answering " + q + " failed", e);
        } finally {
        	kernel.finishQuery(q.getInstance());
        }
    }
    
    private ByteBuffer concatenateByteBuffers(ByteBuffer[] bbs) {
    	// Compute total length of input
    	int size = 0;
    	for (ByteBuffer bb : bbs) {
    		if (null == bb)
    			continue;
    		size += bb.remaining();
    	}
    	
    	ByteBuffer result = ByteBuffer.allocate(size);
    	
    	for (ByteBuffer bb : bbs) {
    		if (null == bb)
    			continue;
    		result.put(bb.duplicate());
    	}
    	
    	result.flip();
    	
    	return result;
    }

    @SuppressWarnings("unchecked")
	public void answerQuery() throws IOException {
        QueryType<?, ?> sp = kernel.getProc(q.getQueryType());
        
        if (null == sp) {
        	log.error("Unknown QueryType id " + q.getQueryType() + 
        			"; aborting query " + q);
        	return;
        }
        
        Serializer serializer = kernel.getSerializer();
        NodeID2InetMapper node2inet = kernel.getNodeID2InetMapper();
        
        ClusterQuerySender querySender = new ClusterQuerySender(kernel, q, serializer, node2inet);
        
        GraphTransaction txn = gdb.startTransaction(GraphTransactionConfig.Standard, querySender);

        // Lookup query's anchor node
        Node anchor = null;
        try {
        	anchor = txn.getNode(q.getNodeId());
        } catch (RuntimeException e) {
        	log.error("Exception while looking up query-anchor-node {}L", q.getNodeId(), e);
        	throw e;
        }
        
        if (null == anchor) {
        	log.error("Looking up query-anchor-node id " + 
        			q.getNodeId() + " returned null");
        	throw new RuntimeException("Failed to look up query-anchor node " + q.getNodeId());
        }

        // Deserialize query object using Kernel serializer
        ByteBuffer qbuf = concatenateByteBuffers(q.getData());
        Object query = serializer.readObjectNotNull(qbuf, sp.queryType());
        
        // Result holder
        GeneralQueryResult<Object> results = new GeneralQueryResult<Object>();
        
        try {
        	((QueryType<Object, Object>)sp).answer(txn, anchor, query, results); // stored procedure call
        } catch (RuntimeException e) {
        	log.debug("Stored procedure generated exception", e);
        	throw e;
        } finally {
            if (txn.isOpen())
                txn.abort();
        }

        int sentResults = 0;
        int skippedResults = 0;
        // Send results to client
        for (Object r : results.getResults()) {
        	if (null == r) {
        		skippedResults++;
        		log.debug("Skipped null result object (" +
        				skippedResults + " total skipped so far)");
        		continue;
        	}
        	// Serialize result object using Kernel serializer
        	DataOutput out = serializer.getDataOutput(SERIALIZER_CAPACITY_GUESS, true);
        	out.writeObjectNotNull(r);
        	// Send serialized result to client
            Result resultMsg = new Result(q.getSeed(), new ByteBuffer[]{out.getByteBuffer()});
            kernel.send(resultMsg, q.getClient());
            sentResults++;
        }

        log.debug("Result totals for " + q.getSeed() + ": " +
        		  "sent " + sentResults + ", " +
        		  "skipped " + skippedResults);

        // Send queued forwards (if any)
        querySender.commit();

        // Tell client we're done working
        Trace t = new Trace(q.getSeed(), q.getInstance(), Trace.Code.END);
        kernel.send(t, q.getClient());
    }

    private static class GeneralQueryResult<T> implements QueryResult<T> {

    	private final List<T> results = new LinkedList<T>();
    	
    	@Override
    	public void add(T result) {
    		results.add(result);
    	}

    	@Override
    	public void close() {
    		// do nothing
    	}
    	
    	public List<T> getResults() {
    		return results;
    	}
    }

}


