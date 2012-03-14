package com.thinkaurelius.titan.graphdb.edges.factory;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.exceptions.ToBeImplementedException;
import com.thinkaurelius.titan.graphdb.adjacencylist.InitialAdjListFactory;
import com.thinkaurelius.titan.graphdb.edges.*;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

public class InMemoryEdgeFactory implements EdgeFactory {

	private GraphTx tx;
	
	public InMemoryEdgeFactory() {
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
				rel = new SimpleBinaryRelationship(type,start,end);
			}
			break;
		case Labeled:
			rel = new LabeledBinaryRelationship(type,start,end,getTx(),InitialAdjListFactory.BasicFactory);
			break;
		default: throw new ToBeImplementedException("HyperEdges are not supported");
		}
		EdgeFactoryUtil.connectEdge(rel, true, getTx());
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
			rel = new SimpleProperty(type,node,attribute);
		} else throw new ToBeImplementedException();
		EdgeFactoryUtil.connectEdge(rel, true, getTx());
		return rel;
	}
	
	@Override
	public InternalEdge createExistingProperty( long id,
			PropertyType type, InternalNode node, Object attribute) {
		throw new UnsupportedOperationException("Not supported for in-memory transactions");
	}

	@Override
	public InternalEdge createExistingProperty(
			PropertyType type, InternalNode node, Object attribute) {
		throw new UnsupportedOperationException("Not supported for in-memory transactions");
	}

	@Override
	public InternalEdge createExistingRelationship( long id, 
			RelationshipType type, InternalNode start, InternalNode end) {
		throw new UnsupportedOperationException("Not supported for in-memory transactions");
	}

	@Override
	public InternalEdge createExistingRelationship(
			RelationshipType type, InternalNode start, InternalNode end) {
		throw new UnsupportedOperationException("Not supported for in-memory transactions");
	}



}
