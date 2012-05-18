package com.thinkaurelius.titan.graphdb.loadingstatus;

import com.thinkaurelius.titan.graphdb.edgequery.InternalTitanQuery;

public interface LoadingStatus {

	public static final LoadingStatus AllLoaded = new DefaultLoadingStatus(true);
	public static final LoadingStatus NothingLoaded = new DefaultLoadingStatus(false);
	
	LoadingStatus loadedEdges(InternalTitanQuery query);
	
	boolean hasLoadedEdges(InternalTitanQuery query);
	
}
