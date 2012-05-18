package com.thinkaurelius.titan.graphdb.edges.factory;

import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.exceptions.InvalidEdgeException;
import com.thinkaurelius.titan.graphdb.edgequery.AtomicTitanQuery;
import com.thinkaurelius.titan.graphdb.edges.InternalRelation;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.tinkerpop.blueprints.Direction;
import static com.tinkerpop.blueprints.Direction.*;

public class EdgeFactoryUtil {
	
	public static final void connectEdge(InternalRelation edge, boolean isNew, InternalTitanTransaction graph) {
		//Check that this edge has not previously been deleted
		if (!isNew && graph.isDeletedRelation(edge)) return;
		
		TitanType et = edge.getType();
		//If functional edge, check that it is indeed unique for that type
		if (isNew && et.isFunctional() && !edge.getVertex(0).isNew()) {
			
			InternalTitanVertex start = (InternalTitanVertex)edge.getVertex(0);
			
			if (hasEdgeOfType(start,et,Direction.OUT)) {
				throw new IllegalArgumentException("Cannot create functional edge since an edge of that type already exists!");
			}
			
		}
		
		int notloaded=0,loaded=0;
        int arity = edge.getArity();
        if (edge.isUnidirected() || edge.isProperty() || edge.isSelfLoop(null)) {
            arity=1;
        }
        for (int i=0;i<arity;i++) {
            if (!edge.getVertex(i).addRelation(edge, isNew)) notloaded++;
            else loaded++;
        }

		
		if (loaded>0) {
			if (notloaded>0) throw new InvalidEdgeException("TitanRelation already existed on some vertices but not on others.");
			else {
				graph.loadedRelation(edge);
			}
		} else if (isNew) {
			throw new InvalidEdgeException("New TitanRelation could not be added.");
		}
	}

	
	public static final boolean hasEdgeOfType(InternalTitanVertex start, TitanType type, Direction dir) {
		AtomicTitanQuery q = new AtomicTitanQuery(start);
		q.includeHidden().type(type).direction(dir);
		if (start.getRelations(q, true).iterator().hasNext()) {
			return true;
		} else {
			return false;
		}
	}
	
}
