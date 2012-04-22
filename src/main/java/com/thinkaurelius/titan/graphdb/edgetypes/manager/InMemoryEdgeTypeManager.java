package com.thinkaurelius.titan.graphdb.edgetypes.manager;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.edgetypes.*;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;

import static com.thinkaurelius.titan.graphdb.edgetypes.manager.EdgeTypeManagerUtil.convertSignature;

public class InMemoryEdgeTypeManager implements EdgeTypeManager  {

	private final EdgeTypeFactory factory;
	
	public InMemoryEdgeTypeManager() {
		factory = new StandardEdgeTypeFactory();
	}
	
	
	@Override
	public PropertyType createPropertyType(GraphTx tx, String name,
			EdgeCategory category, Directionality directionality,
			EdgeTypeVisibility visibility, boolean isfunctional, EdgeType[] keysig, EdgeType[] compactsig,
			EdgeTypeGroup group, boolean isKey, boolean hasIndex,
			Class<?> objectType) {
		StandardPropertyType pt = new StandardPropertyType(name,category,directionality,visibility,
				isfunctional,convertSignature(keysig),convertSignature(compactsig),group,isKey,hasIndex,objectType);
		return factory.createNewPropertyType(pt, tx);
	}

	@Override
	public RelationshipType createRelationshipType(GraphTx tx, String name,
			EdgeCategory category, Directionality directionality,
			EdgeTypeVisibility visibility, boolean isfunctional, EdgeType[] keysig, EdgeType[] compactsig,
			EdgeTypeGroup group) {
		StandardRelationshipType rt = new StandardRelationshipType(name,category,directionality,visibility,
				isfunctional,convertSignature(keysig),convertSignature(compactsig),group);
		return factory.createNewRelationshipType(rt, tx);
	}
	
	@Override
	public EdgeTypeMaker getEdgeTypeMaker(GraphTx tx) {
		return new StandardEdgeTypeMaker(tx,this);
	}

	@Override
	public InternalEdgeType getEdgeType(long id, GraphTx tx) {
		throw new UnsupportedOperationException("Not supported for InMemory Transactions");
	}

	@Override
	public InternalEdgeType getEdgeType(String name, GraphTx tx) {
		throw new UnsupportedOperationException("Not supported for InMemory Transactions");
	}

	@Override
	public void committed(InternalEdgeType edgetype) {
		throw new UnsupportedOperationException("Not supported for InMemory Transactions");
	}

	@Override
	public boolean containsEdgeType(long id, GraphTx tx) {
		return false;
	}

	@Override
	public boolean containsEdgeType(String name, GraphTx tx) {
		return false;
	}
	
	@Override
	public void close() {
		//Nothing to do
	}

}
