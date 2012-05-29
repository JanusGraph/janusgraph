package com.thinkaurelius.titan.graphdb.loadingstatus;

import com.thinkaurelius.titan.graphdb.query.InternalTitanQuery;

public class DefaultLoadingStatus implements LoadingStatus {
	
	private final boolean defaultStatus;
	
	DefaultLoadingStatus(boolean status) {
		defaultStatus = status;
	}

	@Override
	public boolean hasLoadedEdges(InternalTitanQuery query) {
		return defaultStatus;
	}

	@Override
	public LoadingStatus loadedEdges(InternalTitanQuery query) {
		if (!defaultStatus) {
			BasicLoadingStatus update = new BasicLoadingStatus();
			update.loadedEdges(query);
			return update;
		} else return this;
	}

	
}
