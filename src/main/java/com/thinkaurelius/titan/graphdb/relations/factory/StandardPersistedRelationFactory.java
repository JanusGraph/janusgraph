package com.thinkaurelius.titan.graphdb.relations.factory;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.adjacencylist.InitialAdjListFactory;
import com.thinkaurelius.titan.graphdb.relations.InlineTitanEdge;
import com.thinkaurelius.titan.graphdb.relations.InlineProperty;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.graphdb.relations.persist.PersistLabeledTitanEdge;
import com.thinkaurelius.titan.graphdb.relations.persist.PersistSimpleTitanEdge;
import com.thinkaurelius.titan.graphdb.relations.persist.PersistSimpleProperty;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;

public class StandardPersistedRelationFactory implements RelationFactory {

	private InternalTitanTransaction tx;
	
	public StandardPersistedRelationFactory() {
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
	public InternalRelation createExistingProperty( long id,
			TitanKey type, InternalTitanVertex node, Object attribute) {
		Preconditions.checkNotNull(attribute);
		InternalRelation rel=null;
		if (type.isSimple()){
			rel = new PersistSimpleProperty(type,node,attribute,id);
		} else throw new UnsupportedOperationException();
		RelationFactoryUtil.connectRelation(rel, false, getTx());
		return rel;
	}

	@Override
	public InternalRelation createExistingProperty(
			TitanKey type, InternalTitanVertex node, Object attribute) {
		Preconditions.checkNotNull(attribute);
		assert (node instanceof TitanRelation) && !((TitanRelation)node).getType().isSimple();
		InternalRelation rel = new InlineProperty(type,node,attribute);
		RelationFactoryUtil.connectRelation(rel, false, getTx());
		return rel;
	}

	@Override
	public InternalRelation createNewProperty( TitanKey type,
			InternalTitanVertex node, Object attribute) {
		Preconditions.checkNotNull(attribute);
		InternalRelation rel = null;
		if (node instanceof TitanRelation) {
            assert !((TitanRelation)node).getType().isSimple();
			rel = new InlineProperty(type,node,attribute);
		} else if (type.isSimple()){
			rel = new PersistSimpleProperty(type,node,attribute);
		} else throw new UnsupportedOperationException();
        if (!rel.isInline()) getTx().registerNewEntity(rel);
		RelationFactoryUtil.connectRelation(rel, true, getTx());
		return rel;
	}
	
	

	@Override
	public InternalRelation createExistingRelationship( long id,
			TitanLabel type, InternalTitanVertex start, InternalTitanVertex end) {
		InternalRelation rel=null;
		if (type.isSimple()) {
			rel = new PersistSimpleTitanEdge(type,start,end,id);
        } else {
			rel = new PersistLabeledTitanEdge(type,start,end,getTx(),InitialAdjListFactory.BasicFactory,id);
		}
		RelationFactoryUtil.connectRelation(rel, false, getTx());
		return rel;
	}

	@Override
	public InternalRelation createExistingRelationship(
			TitanLabel type, InternalTitanVertex start, InternalTitanVertex end) {
		assert start instanceof TitanRelation && !((TitanRelation)start).getType().isSimple();
		assert type.isUnidirected();
		
		InternalRelation rel=null;
		if (type.isSimple()) {
			rel = new InlineTitanEdge(type,start,end);
        } else throw new IllegalArgumentException("Virtual relationships without ID must be simple");

        RelationFactoryUtil.connectRelation(rel, false, getTx());
		return rel;
	}

	@Override
	public InternalRelation createNewRelationship(
			TitanLabel type, InternalTitanVertex start, InternalTitanVertex end) {
		InternalRelation rel=null;
		if (type.isSimple()) {
			if (start instanceof TitanRelation) {
				assert type.isUnidirected();
                assert !((TitanRelation)start).getType().isSimple();
				rel = new InlineTitanEdge(type,start,end);
			} else {
				rel = new PersistSimpleTitanEdge(type,start,end);
			}
        } else {
			rel = new PersistLabeledTitanEdge(type,start,end,getTx(),InitialAdjListFactory.BasicFactory);
		}
        if (!rel.isInline()) getTx().registerNewEntity(rel);
		RelationFactoryUtil.connectRelation(rel, true, getTx());
		return rel;
	}



}
