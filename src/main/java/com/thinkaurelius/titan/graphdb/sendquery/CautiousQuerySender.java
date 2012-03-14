package com.thinkaurelius.titan.graphdb.sendquery;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.query.QueryType;
import com.thinkaurelius.titan.core.query.ResultCollector;

public class CautiousQuerySender implements QuerySender {

	@Override
	public void abort() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub
		
	}
	

	@Override
	public<T,U> void sendQuery(long nodeid, T queryLoad, 
			Class<? extends QueryType<T,U>> queryType, ResultCollector<U> resultCollector) {
		Preconditions.checkNotNull(queryLoad);
		Preconditions.checkNotNull(queryType);
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
	public void forwardQuery(long nodeid, Object queryLoad) {
		Preconditions.checkNotNull(queryLoad);
		throw new UnsupportedOperationException("Not yet implemented");
	}

}
