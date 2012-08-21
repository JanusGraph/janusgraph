package com.thinkaurelius.titan.graphdb.loadingstatus;

import com.thinkaurelius.titan.graphdb.query.AtomicQuery;

public interface LoadingStatus {

	public static final LoadingStatus AllLoaded = new DefaultLoadingStatus(true);
	public static final LoadingStatus NothingLoaded = new DefaultLoadingStatus(false);
	
	LoadingStatus loadedEdges(AtomicQuery query);
	
	boolean hasLoadedEdges(AtomicQuery query);
	
}
