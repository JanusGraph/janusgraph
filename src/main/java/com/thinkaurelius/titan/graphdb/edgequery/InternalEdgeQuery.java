package com.thinkaurelius.titan.graphdb.edgequery;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.edges.EdgeDirection;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

import java.util.Map;

public interface InternalEdgeQuery extends EdgeQuery {
	
	InternalNode getNode();
	
	InternalEdgeQuery includeHidden();
	
	InternalEdgeQuery copy();



    public long getNodeID();

    boolean hasEdgeTypeCondition();

    EdgeType getEdgeTypeCondition();

    boolean hasEdgeTypeGroupCondition();

    EdgeTypeGroup getEdgeTypeGroupCondition();

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
	
}
