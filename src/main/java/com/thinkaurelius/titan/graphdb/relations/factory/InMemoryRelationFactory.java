package com.thinkaurelius.titan.graphdb.relations.factory;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.adjacencylist.StandardAdjListFactory;
import com.thinkaurelius.titan.graphdb.relations.*;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;

public class InMemoryRelationFactory implements RelationFactory {

	private InternalTitanTransaction tx;
	
	public InMemoryRelationFactory() {
		this.tx=null;
	}
	
	private final InternalTitanTransaction getTx() {
		if (tx==null) throw new IllegalStateException("Factory has not been properly initialized");
		return tx;
	}
	
	@Override
	public void setTransaction(InternalTitanTransaction tx) {
		this.tx=tx;
	}

	@Override
	public InternalRelation createNewRelationship(
			TitanLabel type, InternalTitanVertex start, InternalTitanVertex end) {
		InternalRelation rel=null;
		if (type.isSimple()) {
			if (start instanceof TitanRelation) {
                Preconditions.checkArgument(!((TitanRelation)start).getType().isSimple());
                Preconditions.checkArgument(type.isUnidirected());
                Preconditions.checkArgument(type.isFunctional(),"Inline Edges must be functional: " + type.getName());
				assert type.isUnidirected();
				rel = new InlineTitanEdge(type,start,end);
			} else {
				rel = new SimpleTitanEdge(type,start,end);
			}
        } else {
			rel = new LabeledTitanEdge(type,start,end,getTx(), StandardAdjListFactory.INSTANCE);
		}
        if (!rel.isInline()) getTx().registerNewEntity(rel);
		RelationFactoryUtil.connectRelation(rel, true, getTx());
		return rel;
	}
	
	@Override
	public InternalRelation createNewProperty( TitanKey type,
			InternalTitanVertex node, Object attribute) {
		Preconditions.checkNotNull(attribute);
		InternalRelation rel = null;
		if (node instanceof TitanRelation) {
            Preconditions.checkArgument(!((TitanRelation)node).getType().isSimple());
            Preconditions.checkArgument(type.isFunctional(),"Edge properties must be functional:" + type.getName());
            rel = new InlineProperty(type,node,attribute);
		} else if (type.isSimple()){
			rel = new SimpleProperty(type,node,attribute);
		} else throw new UnsupportedOperationException();
        if (!rel.isInline()) getTx().registerNewEntity(rel);
		RelationFactoryUtil.connectRelation(rel, true, getTx());
		return rel;
	}


}
