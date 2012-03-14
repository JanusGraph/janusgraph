package com.thinkaurelius.titan.core.query;

import com.thinkaurelius.titan.core.GraphTransaction;
import com.thinkaurelius.titan.core.Node;

public interface QueryType<T,U> {
	
	public Class<T> queryType();
	
	public Class<U> resultType();
		
	public void answer(GraphTransaction tx, Node anchor, T query, QueryResult<U> result);
	
}
