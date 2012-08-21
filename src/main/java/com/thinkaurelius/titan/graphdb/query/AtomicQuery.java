package com.thinkaurelius.titan.graphdb.query;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.thinkaurelius.titan.graphdb.vertices.RemovableRelationIterator;
import com.tinkerpop.blueprints.Direction;

import java.util.Iterator;
import java.util.Map;


/**
 * This interface defines the atomic query executable in the Titan backend. Each query
 * must be decomposed into atomic queries for execution.
 * 
 * Currently, AtomicQuery extends TitanQuery since the method overlap is significant. 
 * However, an atomic query does not implement the types/labels/keys methods that TitanQuery
 * provides (those will throw UnsupportedOperationExceptions) since an atomic query only accepts
 * a single type.
 */
public interface AtomicQuery extends TitanQuery {
	
	AtomicQuery type(TitanType type);
    
    AtomicQuery type(String type);
    
	AtomicQuery includeHidden();
	
	AtomicQuery clone();
    
    

    InternalTitanVertex getNode();

    public long getVertexID();

    boolean hasEdgeTypeCondition();

    TitanType getTypeCondition();

    boolean hasGroupCondition();

    TypeGroup getGroupCondition();

    boolean hasDirectionCondition();

    boolean isAllowedDirection(EdgeDirection dir);

    Direction getDirectionCondition();

    boolean queryProperties();

    boolean queryRelationships();

    boolean queryHidden();

    boolean queryUnmodifiable();
    
    boolean hasConstraints();
    
    Map<String,Object> getConstraints();

    long getLimit();

    public VertexListInternal vertexIds();


    public Iterator<TitanProperty> propertyIterator();

    public Iterator<TitanEdge> edgeIterator();

    public Iterator<TitanRelation> relationIterator();

	
}
