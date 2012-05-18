package com.thinkaurelius.titan.graphdb.edges.factory;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.exceptions.ToBeImplementedException;
import com.thinkaurelius.titan.graphdb.adjacencylist.InitialAdjListFactory;
import com.thinkaurelius.titan.graphdb.edges.InlineBinaryTitanEdge;
import com.thinkaurelius.titan.graphdb.edges.InlineProperty;
import com.thinkaurelius.titan.graphdb.edges.InternalRelation;
import com.thinkaurelius.titan.graphdb.edges.persist.PersistLabeledBinaryTitanEdge;
import com.thinkaurelius.titan.graphdb.edges.persist.PersistSimpleBinaryTitanEdge;
import com.thinkaurelius.titan.graphdb.edges.persist.PersistSimpleProperty;
import com.thinkaurelius.titan.graphdb.edgetypes.Directionality;
import com.thinkaurelius.titan.graphdb.edgetypes.EdgeCategory;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;

public class StandardPersistedRelationFactory implements RelationFactory {

	private InternalTitanTransaction tx;
	
	public StandardPersistedRelationFactory() {
		this.tx=null;
	}
	
	private final InternalTitanTransaction getTx() {
		if (tx==null) throw new IllegalStateException("Factory has not been properly initialized!");
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
		} else throw new ToBeImplementedException();
		EdgeFactoryUtil.connectEdge(rel, false, getTx());
		return rel;
	}

	@Override
	public InternalRelation createExistingProperty(
			TitanKey type, InternalTitanVertex node, Object attribute) {
		Preconditions.checkNotNull(attribute);
		assert (node instanceof TitanRelation) && !((TitanRelation)node).getType().isSimple();
		InternalRelation rel = new InlineProperty(type,node,attribute);
		EdgeFactoryUtil.connectEdge(rel, false, getTx());
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
		} else throw new ToBeImplementedException();
        if (!rel.isInline()) getTx().registerNewEntity(rel);
		EdgeFactoryUtil.connectEdge(rel, true, getTx());
		return rel;
	}
	
	

	@Override
	public InternalRelation createExistingRelationship( long id,
			TitanLabel type, InternalTitanVertex start, InternalTitanVertex end) {
		InternalRelation rel=null;
		if (type.isSimple()) {
			rel = new PersistSimpleBinaryTitanEdge(type,start,end,id);
        } else {
			rel = new PersistLabeledBinaryTitanEdge(type,start,end,getTx(),InitialAdjListFactory.BasicFactory,id);
		}
		EdgeFactoryUtil.connectEdge(rel, false, getTx());
		return rel;
	}

	@Override
	public InternalRelation createExistingRelationship(
			TitanLabel type, InternalTitanVertex start, InternalTitanVertex end) {
		assert start instanceof TitanRelation && !((TitanRelation)start).getType().isSimple();
		assert type.isUnidirected();
		
		InternalRelation rel=null;
		if (type.isSimple()) {
			rel = new InlineBinaryTitanEdge(type,start,end);
        } else throw new IllegalArgumentException("Virtual relationships without ID must be simple");

        EdgeFactoryUtil.connectEdge(rel, false, getTx());
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
				rel = new InlineBinaryTitanEdge(type,start,end);
			} else {
				rel = new PersistSimpleBinaryTitanEdge(type,start,end);
			}
        } else {
			rel = new PersistLabeledBinaryTitanEdge(type,start,end,getTx(),InitialAdjListFactory.BasicFactory);
		}
        if (!rel.isInline()) getTx().registerNewEntity(rel);
		EdgeFactoryUtil.connectEdge(rel, true, getTx());
		return rel;
	}



}
