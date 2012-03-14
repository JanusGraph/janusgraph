package com.thinkaurelius.titan.net;

import com.thinkaurelius.titan.core.query.QueryType;
import com.thinkaurelius.titan.graphdb.sendquery.QuerySender;

public interface CommunicationFramework {

	//Constructor which takes a GraphDB object and a NodeID2INet Mapper object, Serializer object,
	
	
	public<T,U> void registerQueryType(QueryType<T, U> queryType);
	
	/**
	 * Creates a new query sender based on this communication framework for sending
	 * new queries issued by the user.
	 * This QuerySender is not linked to an existing query context and hence calling
	 * {@link com.thinkaurelius.titan.graphdb.sendquery.QuerySender#forwardQuery(long, Object)} will through an UnsupportedOperationException().
	 * 
	 * @return New QuerySender instance for sending but not forwarding queries
	 */
	public QuerySender createQuerySender();
	
}
