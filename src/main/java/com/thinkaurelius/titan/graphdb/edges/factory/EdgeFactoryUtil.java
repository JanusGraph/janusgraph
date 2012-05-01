package com.thinkaurelius.titan.graphdb.edges.factory;

import com.thinkaurelius.titan.core.Direction;
import com.thinkaurelius.titan.core.EdgeType;
import com.thinkaurelius.titan.exceptions.InvalidEdgeException;
import com.thinkaurelius.titan.graphdb.edgequery.AtomicEdgeQuery;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

public class EdgeFactoryUtil {
	
	public static final void connectEdge(InternalEdge edge, boolean isNew, GraphTx graph) {
		//Check that this edge has not previously been deleted
		if (!isNew && graph.isDeletedEdge(edge)) return;
		
		EdgeType et = edge.getEdgeType();
		//If functional edge, check that it is indeed unique for that type
		if (isNew && et.isFunctional() && !edge.getStart().isNew()) {
			
			InternalNode start = (InternalNode)edge.getStart();
			
			if (hasEdgeOfType(start,et,Direction.Out)) {
				throw new IllegalArgumentException("Cannot create functional edge since an edge of that type already exists!");
			}
			
		}
		
		int notloaded=0,loaded=0;
        int arity = edge.getArity();
        if (edge.isUnidirected() || edge.isProperty() || edge.isSelfLoop(null)) {
            arity=1;
        }
        for (int i=0;i<arity;i++) {
            if (!edge.getNodeAt(i).addEdge(edge, isNew)) notloaded++;
            else loaded++;
        }

		
		if (loaded>0) {
			if (notloaded>0) throw new InvalidEdgeException("Edge already existed on some vertices but not on others.");
			else {
				graph.loadedEdge(edge);
			}
		} else if (isNew) {
			throw new InvalidEdgeException("New Edge could not be added.");
		}
	}

	
	public static final boolean hasEdgeOfType(InternalNode start, EdgeType type, Direction dir) {
		AtomicEdgeQuery q = new AtomicEdgeQuery(start);
		q.includeHidden().withEdgeType(type).inDirection(dir);
		if (start.getEdges(q, true).iterator().hasNext()) {
			return true;
		} else {
			return false;
		}
	}
	
}
