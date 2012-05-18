package com.thinkaurelius.titan.graphdb.edgetypes.nodes;

import com.thinkaurelius.titan.graphdb.edgetypes.Directionality;
import com.thinkaurelius.titan.graphdb.edgetypes.EdgeCategory;
import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyListFactory;
import com.thinkaurelius.titan.graphdb.edgequery.EdgeQueryUtil;
import com.thinkaurelius.titan.graphdb.edgetypes.InternalTitanType;
import com.thinkaurelius.titan.graphdb.edgetypes.system.SystemKey;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.vertices.PersistStandardTitanVertex;

public abstract class PersistNodeTitanType extends PersistStandardTitanVertex implements InternalTitanType {

	public PersistNodeTitanType(InternalTitanTransaction g, AdjacencyListFactory adjList) {
		super(g,adjList);
	}
	
	public PersistNodeTitanType(InternalTitanTransaction g, AdjacencyListFactory adjList, long id) {
		super(g,adjList,id);
	}

	private String name = null;
	
	@Override
	public String getName() {
		if (name==null) {
			name = EdgeQueryUtil.queryHiddenFunctionalProperty(this, SystemKey.TypeName)
					.getAttribute(String.class);
		}
		return name;
	}
	
	@Override
	public boolean isSimple() {
		return getDefinition().getCategory()==EdgeCategory.Simple;
	}

    public boolean isDirected() {
        return getDefinition().getDirectionality()==Directionality.Directed;
    }

    public boolean isUndirected() {
        return getDefinition().getDirectionality()==Directionality.Undirected;
    }

    public boolean isUnidirected() {
        return getDefinition().getDirectionality()==Directionality.Unidirected;
    }

	@Override
	public TypeGroup getGroup() {
		return getDefinition().getGroup();
	}

	@Override
	public boolean isFunctional() {
		return getDefinition().isFunctional();
	}

	@Override
	public boolean isHidden() {
		return getDefinition().isHidden();
	}

	@Override
	public boolean isModifiable() {
		return getDefinition().isModifiable();
	}


	
	
	
}
