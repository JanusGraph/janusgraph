package com.thinkaurelius.titan.graphdb.vertices;


import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

import java.util.Set;

public abstract class NewEmptyTitanVertex extends LoadedEmptyTitanVertex {


	@Override
	public boolean isLoaded() {
		return false;
	}

    @Override
	public boolean isNew() {
		return true;
	}

}
