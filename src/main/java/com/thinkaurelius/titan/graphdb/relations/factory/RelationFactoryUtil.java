package com.thinkaurelius.titan.graphdb.relations.factory;

import com.thinkaurelius.titan.core.InvalidElementException;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.graphdb.query.AtomicTitanQuery;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.tinkerpop.blueprints.Direction;

public class RelationFactoryUtil {
	
	public static final void connectRelation(InternalRelation relation, boolean isNew, InternalTitanTransaction graph) {
		//Check that this relation has not previously been deleted
		if (!isNew && graph.isDeletedRelation(relation)) return;
		
		TitanType et = relation.getType();
		//If functional relation, check that it is indeed unique for that type
		if (isNew && et.isFunctional() && !relation.getVertex(0).isNew()) {
			
			InternalTitanVertex start = (InternalTitanVertex)relation.getVertex(0);
			
			if (hasRelationOfType(start, et, Direction.OUT)) {
				throw new IllegalArgumentException("Cannot create functional relation since an relation of that type already exists");
			}
			
		}
		
		int notloaded=0,loaded=0;
        int arity = relation.getArity();
        if (relation.isUnidirected() || relation.isProperty() || relation.isLoop()) {
            arity=1;
        }
        for (int i=0;i<arity;i++) {
            if (!relation.getVertex(i).addRelation(relation, isNew)) notloaded++;
            else loaded++;
        }

		
		if (loaded>0) {
			if (notloaded>0) throw new InvalidElementException("Relation already existed on some vertices but not on others",relation);
			else {
				graph.loadedRelation(relation);
			}
		} else if (isNew) {
			throw new InvalidElementException("New relation could not be added",relation);
		}
	}

	
	public static final boolean hasRelationOfType(InternalTitanVertex vertex, TitanType type, Direction dir) {
		AtomicTitanQuery q = new AtomicTitanQuery(vertex);
		q.includeHidden().type(type).direction(dir);
		if (vertex.getRelations(q, true).iterator().hasNext()) {
			return true;
		} else {
			return false;
		}
	}
	
}
