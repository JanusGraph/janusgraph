package com.thinkaurelius.titan.graphdb.sendquery;

import com.thinkaurelius.titan.core.query.QueryType;
import com.thinkaurelius.titan.core.query.ResultCollector;

public class NoQuerySending implements QuerySender {

	public static final QuerySender instance = new NoQuerySending();
	
	private NoQuerySending() {}
	
	@Override
	public void abort() { }

	@Override
	public void commit() { }

	@Override
	public<T,U> void sendQuery(long nodeid, T queryLoad, 
			Class<? extends QueryType<T,U>> queryType, ResultCollector<U> resultCollector) {
		throw new UnsupportedOperationException("Query sending is not supported in this transaction!");
	}

	@Override
	public void forwardQuery(long nodeid, Object queryLoad) {
		throw new UnsupportedOperationException("Query sending is not supported in this transaction!");
	}

}
