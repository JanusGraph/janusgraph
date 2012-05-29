package com.thinkaurelius.titan.graphdb.relations.factory;

import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;

/**
 * The logic to connect edges with vertices (i.e. add edges to the respective vertices' adjacency
 * list) remains outside of the RelationFactory
 * @author matthias
 *
 */
public interface RelationFactory extends RelationLoader {

	InternalRelation createNewProperty(TitanKey type, InternalTitanVertex node, Object attribute);
	
	InternalRelation createNewRelationship(TitanLabel type, InternalTitanVertex start, InternalTitanVertex end);
	
	void setTransaction(InternalTitanTransaction tx);
	

}
