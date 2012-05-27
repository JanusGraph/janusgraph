package com.thinkaurelius.titan.graphdb.edgetypes.manager;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.edgetypes.*;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;

import static com.thinkaurelius.titan.graphdb.edgetypes.manager.EdgeTypeManagerUtil.convertSignature;

public class InMemoryEdgeTypeManager implements EdgeTypeManager  {

	private final EdgeTypeFactory factory;
	
	public InMemoryEdgeTypeManager() {
		factory = new StandardEdgeTypeFactory();
	}
	
	
	@Override
	public TitanKey createPropertyType(InternalTitanTransaction tx, String name,
			EdgeCategory category, Directionality directionality,
			EdgeTypeVisibility visibility, FunctionalType isfunctional, TitanType[] keysig, TitanType[] compactsig,
			TypeGroup group, boolean isKey, boolean hasIndex,
			Class<?> objectType) {
		StandardPropertyType pt = new StandardPropertyType(name,category,directionality,visibility,
				isfunctional,convertSignature(keysig),convertSignature(compactsig),group,isKey,hasIndex,objectType);
		return factory.createNewPropertyKey(pt, tx);
	}

	@Override
	public TitanLabel createRelationshipType(InternalTitanTransaction tx, String name,
			EdgeCategory category, Directionality directionality,
			EdgeTypeVisibility visibility, FunctionalType isfunctional, TitanType[] keysig, TitanType[] compactsig,
			TypeGroup group) {
		StandardRelationshipType rt = new StandardRelationshipType(name,category,directionality,visibility,
				isfunctional,convertSignature(keysig),convertSignature(compactsig),group);
		return factory.createNewEdgeLabel(rt, tx);
	}
	
	@Override
	public TypeMaker getEdgeTypeMaker(InternalTitanTransaction tx) {
		return new StandardTypeMaker(tx,this);
	}

	@Override
	public InternalTitanType getEdgeType(long id, InternalTitanTransaction tx) {
		throw new UnsupportedOperationException("Not supported for InMemory Transactions");
	}

	@Override
	public InternalTitanType getEdgeType(String name, InternalTitanTransaction tx) {
		throw new UnsupportedOperationException("Not supported for InMemory Transactions");
	}

	@Override
	public void committed(InternalTitanType edgetype) {
		throw new UnsupportedOperationException("Not supported for InMemory Transactions");
	}

	@Override
	public boolean containsEdgeType(long id, InternalTitanTransaction tx) {
		return false;
	}

	@Override
	public boolean containsEdgeType(String name, InternalTitanTransaction tx) {
		return false;
	}
	
	@Override
	public void close() {
		//Nothing to do
	}

}
