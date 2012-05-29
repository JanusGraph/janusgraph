package com.thinkaurelius.titan.graphdb.relations.factory;

import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;


/**
 * Factory interface for creation of existing edges.
 * 
 * An edge (property or relationship) <b>exists</b> if it is persisted to external memory. Calling these
 * factory methods creates an in-memory representation of such persisted edges.
 * 
 * If existing edges do not have an id, the are <b>inline</b> edges, i.e. they are not persisted in their own right
 * but inline with another edge.
 *
 */
public interface RelationLoader {

	InternalRelation createExistingProperty(long id, TitanKey type, InternalTitanVertex node, Object attribute);

	InternalRelation createExistingProperty(TitanKey type, InternalTitanVertex node, Object attribute);


	InternalRelation createExistingRelationship(TitanLabel type, InternalTitanVertex start, InternalTitanVertex end);
	
	InternalRelation createExistingRelationship(long id, TitanLabel type, InternalTitanVertex start, InternalTitanVertex end);


}
