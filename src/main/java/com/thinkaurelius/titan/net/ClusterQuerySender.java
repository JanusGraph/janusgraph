package com.thinkaurelius.titan.net;

import com.thinkaurelius.titan.core.query.QueryType;
import com.thinkaurelius.titan.core.query.ResultCollector;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.sendquery.QuerySender;
import com.thinkaurelius.titan.net.msg.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;


public class ClusterQuerySender implements QuerySender {

	private final Kernel kernel;
	private final Query query;
	private final Serializer serializer;
	private final NodeID2InetMapper node2inet;
	private final List<F> storedForwards = new LinkedList<F>();
	
	private static final int SERIALIZER_CAPACITY_GUESS = 128;
	private static final Logger logger = LoggerFactory.getLogger(ClusterQuerySender.class);

	
	public ClusterQuerySender(Kernel kernel, Query query, Serializer serializer, NodeID2InetMapper node2inet) {
		this.kernel = kernel;
		this.query = query;
		this.serializer = serializer;
		this.node2inet = node2inet;
	}
	
	@Override
	public <T, U> void sendQuery(long nodeid, T queryLoad,
			Class<? extends QueryType<T, U>> queryType,
			ResultCollector<U> resultCollector) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Sending a query from within the cluster is not yet supported.");
	}

	@Override
	public void forwardQuery(long nodeid, Object queryLoad) {
		F fwd = new F(nodeid, queryLoad);
		storedForwards.add(fwd);
	}

	@Override
	public void commit() {
		for (F fwd : storedForwards) {
			doForward(fwd);
		}
	}

	@Override
	public void abort() {
		// Do nothing
	}

	private void doForward(F f) {
		InetAddress[] candidates = node2inet.getInetAddress(f.nodeid);
		if (null == candidates) {
			logger.debug("Could not determine forwarding candidates for anchor id " + f.nodeid);
			return;
		}
		DataOutput out = 
			serializer.getDataOutput(SERIALIZER_CAPACITY_GUESS, true);
		out.writeObjectNotNull(f.queryLoad);
		ByteBuffer rawData[] = new ByteBuffer[]{out.getByteBuffer()};
		// map candidates array to list of InetSocketAddress (assuming remote Titans use same port as local Titan)
		List<InetSocketAddress> dests = new LinkedList<InetSocketAddress>();
		for (InetAddress a : candidates) {
			dests.add(new InetSocketAddress(a, kernel.getListenAddress().getPort()));
		}
		// TODO hide IndividualQuerySender existence behind a Kernel method?
		Query forwardableQuery = 
			query.spawnNextGeneration(kernel.generateKey(), rawData, f.nodeid);
		IndividualQuerySender fwdSender = 
			new IndividualQuerySender(kernel, forwardableQuery, dests);
		kernel.sendQuery(fwdSender);
	}
	
	private static class F {
		final long nodeid;
		final Object queryLoad;
		
		public F(long nodeid, Object queryLoad) {
			this.nodeid = nodeid;
			this.queryLoad = queryLoad;
		}
	}
}