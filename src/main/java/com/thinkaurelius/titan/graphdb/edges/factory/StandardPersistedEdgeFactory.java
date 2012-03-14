package com.thinkaurelius.titan.graphdb.edges.factory;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.exceptions.ToBeImplementedException;
import com.thinkaurelius.titan.graphdb.adjacencylist.InitialAdjListFactory;
import com.thinkaurelius.titan.graphdb.edges.InlineBinaryRelationship;
import com.thinkaurelius.titan.graphdb.edges.InlineProperty;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.edges.persist.PersistLabeledBinaryRelationship;
import com.thinkaurelius.titan.graphdb.edges.persist.PersistSimpleBinaryRelationship;
import com.thinkaurelius.titan.graphdb.edges.persist.PersistSimpleProperty;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

public class StandardPersistedEdgeFactory implements EdgeFactory {

	private GraphTx tx;
	
	public StandardPersistedEdgeFactory() {
		this.tx=null;
	}
	
	private final GraphTx getTx() {
		if (tx==null) throw new IllegalStateException("Factory has not been properly initialized!");
		return tx;
	}
	
	@Override
	public void setTransaction(GraphTx tx) {
		this.tx=tx;
	}

	@Override
	public InternalEdge createExistingProperty( long id,
			PropertyType type, InternalNode node, Object attribute) {
		Preconditions.checkNotNull(attribute);
		InternalEdge rel=null;
		if (type.getCategory()==EdgeCategory.Simple){
			rel = new PersistSimpleProperty(type,node,attribute,id);
		} else throw new ToBeImplementedException();
		EdgeFactoryUtil.connectEdge(rel, false, getTx());
		return rel;
	}

	@Override
	public InternalEdge createExistingProperty(
			PropertyType type, InternalNode node, Object attribute) {
		Preconditions.checkNotNull(attribute);
		assert (node instanceof Edge) && ((Edge)node).getEdgeType().getCategory().isLabeled();
		InternalEdge rel = new InlineProperty(type,node,attribute);
		EdgeFactoryUtil.connectEdge(rel, false, getTx());
		return rel;
	}

	@Override
	public InternalEdge createNewProperty( PropertyType type,
			InternalNode node, Object attribute) {
		Preconditions.checkNotNull(attribute);
		InternalEdge rel = null;
		if ((node instanceof Edge) && 
				((Edge)node).getEdgeType().getCategory().isLabeled()) {
			rel = new InlineProperty(type,node,attribute);
		} else if (type.getCategory()==EdgeCategory.Simple){
			rel = new PersistSimpleProperty(type,node,attribute);
		} else throw new ToBeImplementedException();
		EdgeFactoryUtil.connectEdge(rel, true, getTx());
		return rel;
	}
	
	

	@Override
	public InternalEdge createExistingRelationship( long id, 
			RelationshipType type, InternalNode start, InternalNode end) {
		InternalEdge rel=null;
		switch(type.getCategory()) {
		case Simple:
			rel = new PersistSimpleBinaryRelationship(type,start,end,id);
			break;
		case Labeled:
		case LabeledRestricted:
			rel = new PersistLabeledBinaryRelationship(type,start,end,getTx(),InitialAdjListFactory.BasicFactory,id);
			break;
		default: throw new AssertionError("Unsupported Edge type: " + type.getCategory());
		}
		EdgeFactoryUtil.connectEdge(rel, false, getTx());
		return rel;
	}

	@Override
	public InternalEdge createExistingRelationship(
			RelationshipType type, InternalNode start, InternalNode end) {
		assert start instanceof Edge && ((Edge)start).getEdgeType().getCategory().isLabeled();
		assert type.getDirectionality()==Directionality.Unidirected;
		
		InternalEdge rel=null;
		switch(type.getCategory()) {
		case Simple:
			rel = new InlineBinaryRelationship(type,start,end);
			break;
		default: throw new IllegalArgumentException("Virtual relationships without ID must be simple");
		}
		EdgeFactoryUtil.connectEdge(rel, false, getTx());
		return rel;
	}

	@Override
	public InternalEdge createNewRelationship(
			RelationshipType type, InternalNode start, InternalNode end) {
		InternalEdge rel=null;
		switch(type.getCategory()) {
		case Simple:
			if ((start instanceof Edge) && 
					((Edge)start).getEdgeType().getCategory().isLabeled()) {
				assert type.getDirectionality()==Directionality.Unidirected;
				rel = new InlineBinaryRelationship(type,start,end);
			} else {
				rel = new PersistSimpleBinaryRelationship(type,start,end);
			}
			break;
		case Labeled:
			rel = new PersistLabeledBinaryRelationship(type,start,end,getTx(),InitialAdjListFactory.BasicFactory);
			break;
        default: throw new AssertionError("Unsupported Edge type: " + type.getCategory());
		}
		EdgeFactoryUtil.connectEdge(rel, true, getTx());
		return rel;
	}



}
