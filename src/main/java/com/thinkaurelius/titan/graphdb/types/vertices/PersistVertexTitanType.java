package com.thinkaurelius.titan.graphdb.types.vertices;

import com.thinkaurelius.titan.graphdb.types.Directionality;
import com.thinkaurelius.titan.graphdb.types.TypeCategory;
import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyListFactory;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.types.InternalTitanType;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.vertices.PersistStandardTitanVertex;

public abstract class PersistVertexTitanType extends PersistStandardTitanVertex implements InternalTitanType {

	public PersistVertexTitanType(InternalTitanTransaction g, AdjacencyListFactory adjList) {
		super(g,adjList);
	}
	
	public PersistVertexTitanType(InternalTitanTransaction g, AdjacencyListFactory adjList, long id) {
		super(g,adjList,id);
	}

	private String name = null;
	
	@Override
	public String getName() {
		if (name==null) {
			name = QueryUtil.queryHiddenFunctionalProperty(this, SystemKey.TypeName)
					.getAttribute(String.class);
		}
		return name;
	}
	
	@Override
	public boolean isSimple() {
		return getDefinition().getCategory()== TypeCategory.Simple;
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
    public boolean isFunctionalLocking() {
        return getDefinition().isFunctionalLocking();
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
