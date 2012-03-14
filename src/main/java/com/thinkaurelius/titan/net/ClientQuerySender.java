package com.thinkaurelius.titan.net;

import com.thinkaurelius.titan.core.query.QueryType;
import com.thinkaurelius.titan.core.query.ResultCollector;
import com.thinkaurelius.titan.graphdb.sendquery.QuerySender;

public class ClientQuerySender implements QuerySender {

	private final Kernel kernel;
	
	public ClientQuerySender(Kernel kernel) {
		this.kernel = kernel;
	}
	
	@Override
	public <T, U> void sendQuery(long nodeid, T queryLoad,
			Class<? extends QueryType<T, U>>  queryType,
			ResultCollector<U> resultCollector) {
		kernel.sendQuery(nodeid, queryLoad, queryType, resultCollector);
	}

	@Override
	public void forwardQuery(long nodeid, Object queryLoad) {
		throw new UnsupportedOperationException("Forwarding a query from a client QuerySender is not supported");
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void abort() {
		// TODO Auto-generated method stub	
	}
}
