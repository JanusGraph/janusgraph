package com.thinkaurelius.titan.graphdb.query;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.tinkerpop.blueprints.Direction;

import java.util.Map;

public interface InternalTitanQuery extends TitanQuery {
	
	InternalTitanVertex getNode();
	
	InternalTitanQuery includeHidden();
	
	InternalTitanQuery copy();

    public boolean isAtomic();



    public long getNodeID();

    boolean hasEdgeTypeCondition();

    TitanType getTypeCondition();

    boolean hasEdgeTypeGroupCondition();

    TypeGroup getEdgeTypeGroupCondition();

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
