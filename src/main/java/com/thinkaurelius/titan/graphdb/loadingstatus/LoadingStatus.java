package com.thinkaurelius.titan.graphdb.loadingstatus;

import com.thinkaurelius.titan.graphdb.edgequery.InternalEdgeQuery;

public interface LoadingStatus {

	public static final LoadingStatus AllLoaded = new DefaultLoadingStatus(true);
	public static final LoadingStatus NothingLoaded = new DefaultLoadingStatus(false);
	
	LoadingStatus loadedEdges(InternalEdgeQuery query);
	
	boolean hasLoadedEdges(InternalEdgeQuery query);
	
}
