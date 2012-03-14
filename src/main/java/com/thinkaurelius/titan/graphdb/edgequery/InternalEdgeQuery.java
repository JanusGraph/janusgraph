package com.thinkaurelius.titan.graphdb.edgequery;

import com.thinkaurelius.titan.core.Direction;
import com.thinkaurelius.titan.core.EdgeQuery;
import com.thinkaurelius.titan.core.EdgeType;
import com.thinkaurelius.titan.core.EdgeTypeGroup;
import com.thinkaurelius.titan.graphdb.edges.EdgeDirection;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

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

    long getLimit();

    boolean returnPartialResult();
	
}
