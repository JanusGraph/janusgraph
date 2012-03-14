package com.thinkaurelius.titan.graphdb.sendquery;

import com.thinkaurelius.titan.core.query.QueryType;
import com.thinkaurelius.titan.core.query.ResultCollector;

public interface QuerySender {

	public<T,U> void sendQuery(long nodeid, T queryLoad, Class<? extends QueryType<T, U>> queryType, ResultCollector<U> resultCollector);
	
	public void forwardQuery(long nodeid, Object queryLoad);
	
	public void commit();
	
	public void abort();
	
}
